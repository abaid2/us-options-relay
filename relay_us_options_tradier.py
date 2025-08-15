#!/usr/bin/env python3
# Clean relay — Tradier options + optional Finnhub earnings
# Publishes:
#   - market.json   (underliers + selected option quotes + greeks + chain metrics + EMA + caches)
#   - history.json  (12–20 realized earnings comps per Tier-B symbol; only if FINNHUB_TOKEN)
#   - calendar.json (optional; only if GIST_ID_CALENDAR is set; contains Tier-A and Tier-B earnings list)
#
# Design goals: fast, robust, minimal external dependencies.

import os, json, re, urllib.parse
import datetime as dt
from datetime import timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ── ENV ──────────────────────────────────────────────────────────────────────
TRADIER_TOKEN   = os.environ.get("TRADIER_TOKEN")
TRADIER_BASE    = os.environ.get("TRADIER_BASE", "https://api.tradier.com/v1")
FINNHUB_TOKEN   = os.environ.get("FINNHUB_TOKEN", "")  # optional (for earnings/history)
GIST_TOKEN      = os.environ.get("GIST_TOKEN")
GIST_ID_MARKET  = os.environ.get("GIST_ID_MARKET")
GIST_ID_HISTORY = os.environ.get("GIST_ID_HISTORY")
GIST_ID_CAL     = os.environ.get("GIST_ID_CALENDAR", "")  # optional; empty = skip calendar.json

# Tuning
INTERVAL_SEC   = int(os.environ.get("INTERVAL_SEC",   "300"))  # meta tag only
RELAY_WORKERS  = int(os.environ.get("RELAY_WORKERS",  "8"))
TIERB_LIMIT    = int(os.environ.get("TIERB_LIMIT",    "40"))   # cap Tier-B to lighten load (0 = no cap)

# Required envs
assert TRADIER_TOKEN and TRADIER_BASE and GIST_TOKEN and GIST_ID_MARKET and GIST_ID_HISTORY, "Missing env vars."

# ── HEADERS / TZ ─────────────────────────────────────────────────────────────
HDR_TR = {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"}
HDR_GH = {"Authorization": f"Bearer {GIST_TOKEN}",    "Accept": "application/vnd.github+json"}
UTC    = timezone.utc

# ── UNIVERSE ─────────────────────────────────────────────────────────────────
TIER_A = [
    "SPX","XSP","SPY","QQQ","TSLA","NVDA","AAPL","MSFT","META",
    "AMD","GOOGL","NFLX","AVGO","IWM","SMH","XBI","TLT","GDX"
]

# ── HELPERS ──────────────────────────────────────────────────────────────────
def now_utc_iso() -> str:
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat()

def http_get_json(url, headers=None, params=None, timeout=25):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def gh_put(gist_id: str, filename: str, obj):
    url = f"https://api.github.com/gists/{gist_id}"
    payload = {"files": {filename: {"content": json.dumps(obj, indent=2)}}}
    r = requests.patch(url, headers=HDR_GH, json=payload, timeout=25)
    r.raise_for_status()

def gh_get(gist_id: str, filename: str):
    try:
        j = http_get_json(f"https://api.github.com/gists/{gist_id}", headers=HDR_GH)
        f = (j.get("files") or {}).get(filename)
        if not f or "content" not in f:
            return None
        return json.loads(f["content"])
    except Exception:
        return None

# ── TRADIER (prices & chains) ────────────────────────────────────────────────
def quote_underlier(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/quotes", headers=HDR_TR, params={"symbols": sym})
    q = (j.get("quotes") or {}).get("quote")
    if not q: return None
    if isinstance(q, list): q = q[0]
    last = q.get("last") or q.get("close")
    if not last and q.get("bid") and q.get("ask"):
        last = (float(q["bid"]) + float(q["ask"])) / 2
    return float(last) if last is not None else None

def daily_history(sym, start):
    j = http_get_json(f"{TRADIER_BASE}/markets/history", headers=HDR_TR,
                      params={"symbol": sym, "interval": "daily", "start": start})
    days = (j.get("history") or {}).get("day") or []
    return [days] if isinstance(days, dict) else days

def atr_from_history(days, period):
    if len(days) < period + 1: return None
    trs, prev = [], float(days[0]["close"])
    for d in days[1:]:
        h, l, c = float(d["high"]), float(d["low"]), float(d["close"])
        trs.append(max(h - l, abs(h - prev), abs(l - prev)))
        prev = c
    return sum(trs[-period:]) / period if len(trs) >= period else None

def expirations(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/expirations", headers=HDR_TR,
                      params={"symbol": sym, "includeAllRoots": "true", "strikes": "false"})
    exps = (j.get("expirations") or {}).get("date") or []
    return [exps] if isinstance(exps, str) else sorted(exps)

def chains(sym, exp):
    # greeks=true → ORATS greeks + IV on chain rows (hourly-ish), good fallback
    j = http_get_json(f"{TRADIER_BASE}/markets/options/chains", headers=HDR_TR,
                      params={"symbol": sym, "expiration": exp, "greeks": "true"})
    opts = (j.get("options") or {}).get("option") or []
    return [opts] if isinstance(opts, dict) else opts

# Quotes batching (URL-length aware + binary split + OCC validation)
_OCC_RE         = re.compile(r'^[A-Z.]{1,6}\d{6}[CP]\d{8}$')
_OCC_TYPE_RE    = re.compile(r'\d{6}([CP])\d{8}$')
_OCC_STRIKE_RE  = re.compile(r'(\d{8})$')
_MAX_URL        = 1500

def _is_valid_occ(sym: str) -> bool:
    return bool(sym) and bool(_OCC_RE.match(sym))

def _occ_type(sym: str):
    m = _OCC_TYPE_RE.search(sym or "")
    return ("call" if m.group(1) == "C" else "put") if m else None

def _occ_strike(sym: str):
    m = _OCC_STRIKE_RE.search(sym or "")
    try: return int(m.group(1))/1000.0 if m else None
    except: return None

def _fetch_quotes_chunk(chunk_syms):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/quotes", headers=HDR_TR,
                      params={"symbols": ",".join(chunk_syms), "greeks": "true"})
    rows = (j.get("quotes") or {}).get("quote") or []
    return [rows] if isinstance(rows, dict) else rows

def quotes_options(symbols):
    out = []
    if not symbols: return out
    # de-dup & validate
    seen, syms = set(), []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            if _is_valid_occ(s): syms.append(s)
    i = 0
    while i < len(syms):
        chunk = []
        while i < len(syms):
            test = ",".join(chunk + [syms[i]])
            q = urllib.parse.urlencode({"symbols": test, "greeks": "true"})
            if len(f"{TRADIER_BASE}/markets/options/quotes?{q}") > _MAX_URL:
                if not chunk: chunk = [syms[i]]; i += 1
                break
            chunk.append(syms[i]); i += 1
        try:
            out.extend(_fetch_quotes_chunk(chunk))
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (404, 414):
                stack = [chunk]
                while stack:
                    part = stack.pop()
                    if len(part) == 1:
                        try: out.extend(_fetch_quotes_chunk(part))
                        except requests.HTTPError as e2:
                            if getattr(e2.response, "status_code", None) in (404,414,400): continue
                            raise
                    else:
                        mid = len(part)//2
                        stack.append(part[:mid]); stack.append(part[mid:])
                continue
            raise
    return out

# ── Selection & metrics ──────────────────────────────────────────────────────
def nearest_friday_on_or_after(date_str):
    y, m, d = map(int, date_str.split("-"))
    base = dt.date(y, m, d)
    add = (4 - base.weekday()) % 7  # Friday=4
    return (base + timedelta(days=add)).isoformat()

def pick_expiration(sym, desired):
    exps = expirations(sym)
    if not exps: return None
    for e in exps:
        if e >= desired: return e
    return exps[-1]

def pick_targets(chain_rows, spot, how_many=2):
    calls = [c for c in chain_rows if (c.get("option_type") or "").lower() == "call"]
    puts  = [p for p in chain_rows if (p.get("option_type") or "").lower() == "put"]
    calls.sort(key=lambda x: abs(float(x.get("strike", 0)) - spot))
    puts.sort(key=lambda x: abs(float(x.get("strike", 0)) - spot))
    best, bestd = None, 1e12
    for c in calls[:how_many*3]:
        cs = float(c.get("strike", 0))
        if not puts: break
        pm = min(puts[:how_many*3], key=lambda p: abs(float(p.get("strike", 0)) - cs))
        d  = abs(cs - spot) + abs(float(pm.get("strike", 0)) - spot)
        if pm and d < bestd:
            bestd, best = d, (c, pm)
    wings = []
    for i in range(1, how_many + 1):
        if i < len(calls) and i < len(puts):
            wings.append((calls[i], puts[i]))
    return {"atm": best, "wings": wings}

def spread_pct(bid, ask):
    try:
        bid = float(bid) if bid is not None else None
        ask = float(ask) if ask is not None else None
        if not bid or not ask: return None
        mid = (bid + ask) / 2.0
        return (ask - bid) / mid if mid > 0 else None
    except Exception:
        return None

def chain_metrics_from_chain(chain_rows):
    oi_sum, spreads, notional = 0, [], 0.0
    for o in chain_rows:
        try:
            oi  = int(o.get("open_interest") or 0)
            vol = int(o.get("volume") or 0)
            bid = float(o.get("bid")) if o.get("bid") is not None else None
            ask = float(o.get("ask")) if o.get("ask") is not None else None
            oi_sum += oi
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                spreads.append((ask - bid)/mid)
                if vol > 0: notional += vol * mid * 100.0
        except Exception:
            continue
    spreads.sort()
    med_spread = spreads[len(spreads)//2] if spreads else None
    return {"chain_oi_total": oi_sum, "chain_median_spread_pct": med_spread, "chain_notional_today": notional}

def ema_update(prev, x, alpha=0.15):
    return x if prev is None else (alpha*x + (1-alpha)*prev)

# ── FINNHUB (optional earnings & history) ────────────────────────────────────
def fetch_earnings(from_date, to_date):
    if not FINNHUB_TOKEN: return []
    try:
        j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                          params={"from": from_date, "to": to_date, "token": FINNHUB_TOKEN})
    except Exception:
        return []
    out = []
    for x in (j.get("earningsCalendar") or []):
        sym = (x.get("symbol") or "").upper()
        if sym and x.get("date"):
            when = (x.get("hour") or x.get("time") or "").upper()
            out.append({"symbol": sym, "date": x["date"], "when": when})
    return out

def fh_earnings_history(symbol, max_quarters=20):
    if not FINNHUB_TOKEN: return []
    dates = []
    try:
        j = http_get_json("https://finnhub.io/api/v1/stock/earnings",
                          params={"symbol": symbol, "token": FINNHUB_TOKEN})
        for r in (j or []):
            d = r.get("date") or r.get("period")
            if d: dates.append(str(d)[:10])
    except Exception:
        dates = []
    dates = sorted(set(dates), reverse=True)[:max_quarters]
    if not dates:
        try:
            end   = dt.datetime.now(UTC).date()
            start = (end - timedelta(days=3*365))
            j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                              params={"from": start.isoformat(), "to": end.isoformat(), "token": FINNHUB_TOKEN})
            for x in (j.get("earningsCalendar") or []):
                if (x.get("symbol") or "").upper() == symbol and x.get("date"):
                    dates.append(x["date"])
        except Exception:
            pass
    return sorted(set(dates), reverse=True)[:max_quarters]

def compute_comps(symbol, earn_dates):
    # gap% + D/D+1 range%, using Tradier daily bars
    start = (dt.datetime.now(UTC) - timedelta(days=3*365)).date().isoformat()
    days  = daily_history(symbol, start)
    if not days: return []
    idx = {str(d["date"]): {"o": float(d["open"]), "h": float(d["high"]), "l": float(d["low"]), "c": float(d["close"])} for d in days}
    out = []
    for d in earn_dates:
        d0 = dt.date.fromisoformat(d)
        # find nearest trading D-1 and D+1
        d_1 = next(( (d0 - timedelta(days=k)).isoformat() for k in range(1,4) if (d0 - timedelta(days=k)).isoformat() in idx ), None)
        d1  = next(( (d0 + timedelta(days=k)).isoformat() for k in range(1,4) if (d0 + timedelta(days=k)).isoformat() in idx ), None)
        bar_d0 = idx.get(d)
        if not bar_d0: continue
        # assume BMO if unknown
        if d_1:
            c_1 = idx[d_1]["c"]; o0 = bar_d0["o"]; h0 = bar_d0["h"]; l0 = bar_d0["l"]
            gap = abs(o0 - c_1)/c_1
            rng = (h0 - l0)/c_1
            out.append({"date": d, "when": "BMO", "gap_pct": round(gap,6), "range_pct": round(rng,6)})
        elif d1:
            c0 = bar_d0["c"]; o1 = idx[d1]["o"]; h1 = idx[d1]["h"]; l1 = idx[d1]["l"]
            gap = abs(o1 - c0)/c0
            rng = (h1 - l1)/c0
            out.append({"date": d, "when": "AMC", "gap_pct": round(gap,6), "range_pct": round(rng,6)})
    return out

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    now       = dt.datetime.now(UTC)
    from_date = now.date().isoformat()
    to_date   = (now + timedelta(days=14)).date().isoformat()

    # Optional earnings → Tier-B
    earn = fetch_earnings(from_date, to_date) if FINNHUB_TOKEN else []
    if TIERB_LIMIT and TIERB_LIMIT > 0 and len(earn) > TIERB_LIMIT:
        earn = sorted(earn, key=lambda x: x["date"])[:TIERB_LIMIT]
    tierB_syms = sorted({e["symbol"] for e in earn})
    symbols    = sorted(set(TIER_A + tierB_syms))
    print(f"[relay] symbols={len(symbols)} tierB={len(tierB_syms)}")

    # Optional history.json (only if FINNHUB_TOKEN)
    if FINNHUB_TOKEN and tierB_syms:
        hist = gh_get(GIST_ID_HISTORY, "history.json") or {"meta":{}, "symbols":{}}
        symmap, changed = hist.setdefault("symbols", {}), False
        for s in tierB_syms:
            last_ts = (symmap.get(s) or {}).get("_updated_utc")
            stale = True
            if last_ts:
                try:
                    dt_last = dt.datetime.fromisoformat(last_ts.replace("Z","+00:00"))
                    stale = (now - dt_last).days >= 7
                except Exception:
                    stale = True
            if not stale: continue
            dates = fh_earnings_history(s, max_quarters=20)
            if not dates: continue
            comps = compute_comps(s, dates)
            if comps:
                symmap[s] = {"_updated_utc": now_utc_iso(), "comps": comps}
                changed = True
        if changed:
            hist["meta"] = {"updated_utc": now_utc_iso(), "lookback_years": 3}
            gh_put(GIST_ID_HISTORY, "history.json", hist)

    # Optional calendar.json (if GIST_ID_CAL present)
    if GIST_ID_CAL:
        cal_payload = {
            "meta":  {"source": "finnhub" if FINNHUB_TOKEN else "none",
                      "updated_utc": now_utc_iso(),
                      "window_from": from_date, "window_to": to_date},
            "tierA": TIER_A,
            "tierB": earn
        }
        gh_put(GIST_ID_CAL, "calendar.json", cal_payload)

    # Load previous market state (EMA, last_good, ATR cache)
    prev_market    = gh_get(GIST_ID_MARKET, "market.json") or {}
    prev_state     = prev_market.get("state", {}) if isinstance(prev_market, dict) else {}
    prev_ema       = prev_state.get("ema", {}) if isinstance(prev_state, dict) else {}
    prev_last_good = prev_state.get("last_good_quotes", {}) if isinstance(prev_state, dict) else {}
    prev_atr       = prev_state.get("atr_cache", {}) if isinstance(prev_state, dict) else {}

    # Per-symbol work (parallel)
    hist_start   = (now - timedelta(days=120)).date().isoformat()
    atr_updates  = {}
    underliers   = []
    chain_metrics= []
    sel_meta     = []
    opt_symbols  = []
    chain_fallback_all = {}

    def process_symbol(u):
        try:
            spot = quote_underlier(u)
            if not spot: return None

            # ATR (cache per day)
            cache = prev_atr.get(u)
            if cache and cache.get("asof") == now.date().isoformat():
                atr5, atr14 = cache.get("atr5"), cache.get("atr14")
            else:
                days = daily_history(u, hist_start)
                atr5  = atr_from_history(days[-40:], 5) if days else None
                atr14 = atr_from_history(days[-80:],14) if days else None
                atr_updates[u] = {"asof": now.date().isoformat(), "atr5": atr5, "atr14": atr14}

            # pick expiry
            e_dates = [x["date"] for x in earn if x["symbol"] == u]
            desired = nearest_friday_on_or_after(e_dates[0]) if e_dates else nearest_friday_on_or_after((now + timedelta(days=7)).date().isoformat())
            expiry  = pick_expiration(u, desired)
            if not expiry:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}}

            chain_rows = chains(u, expiry)
            if not chain_rows:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}}

            cm = chain_metrics_from_chain(chain_rows)

            # Chain prefilter (hard gates)
            if cm.get("chain_oi_total", 0) < 15000:  # chain-wide OI
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()},
                        "cm": {"u": u, "expiry": expiry, **cm}}

            ms = cm.get("chain_median_spread_pct")
            if ms is not None and ms > 0.10:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()},
                        "cm": {"u": u, "expiry": expiry, **cm}}

            picks = pick_targets(chain_rows, spot, how_many=2)
            legs = []
            if picks["atm"]: legs += list(picks["atm"])
            for c_leg, p_leg in picks["wings"]:
                legs += [c_leg, p_leg]

            # chain fallback (bid/ask/oi/volume + greeks)
            def fnum(x):
                try: return float(x)
                except: return None
            fallback = {}
            leg_syms = {l.get("symbol") for l in legs if l.get("symbol")}
            for o in chain_rows:
                if o.get("symbol") in leg_syms:
                    g = o.get("greeks") or {}
                    fallback[o["symbol"]] = {
                        "bid": fnum(o.get("bid")), "ask": fnum(o.get("ask")),
                        "volume": int(o.get("volume")) if o.get("volume") is not None else None,
                        "oi": int(o.get("open_interest")) if o.get("open_interest") is not None else None,
                        "iv": fnum(g.get("mid_iv")) or fnum(g.get("smv_vol")),
                        "delta": fnum(g.get("delta")), "gamma": fnum(g.get("gamma")),
                        "theta": fnum(g.get("theta")), "vega": fnum(g.get("vega")),
                    }

            under = {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}
            sel   = [{"u": u, "expiry": expiry, "type": ("C" if (l.get("option_type") or _occ_type(l.get("symbol")))=="call" else "P"),
                      "strike": float(l.get("strike") or _occ_strike(l.get("symbol"))),
                      "contract": l.get("symbol")} for l in legs if l.get("symbol")]
            return {"underlier": under, "cm": {"u": u, "expiry": expiry, **cm},
                    "sel": sel, "fallback": fallback}
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=RELAY_WORKERS) as ex:
        for fut in as_completed({ex.submit(process_symbol, u): u for u in symbols}):
            r = fut.result()
            if not r: continue
            if r.get("underlier"): underliers.append(r["underlier"])
            if r.get("cm"):        chain_metrics.append(r["cm"])
            if r.get("sel"):
                sel_meta.extend(r["sel"])
                opt_symbols.extend([s["contract"] for s in r["sel"]])
            if r.get("fallback"):  chain_fallback_all.update(r["fallback"])

    # Quotes for selected legs
    qts   = quotes_options(opt_symbols)
    bysym = {q.get("symbol"): q for q in qts} if qts else {}

    # Build quotes with fallbacks; persist greeks
    last_good = dict(prev_last_good)
    quotes    = []
    for m in sel_meta:
        q   = bysym.get(m["contract"], {})
        bid = q.get("bid"); ask = q.get("ask"); last = q.get("last")
        oi  = q.get("open_interest"); vol = q.get("volume")
        # greeks
        def f(x):
            try: return float(x)
            except: return None
        delta = gamma = theta = vega = None
        iv = None
        if q.get("greeks"):
            g = q["greeks"]
            iv    = f(g.get("mid_iv")) or f(g.get("smv_vol"))
            delta = f(g.get("delta")); gamma = f(g.get("gamma"))
            theta = f(g.get("theta")); vega  = f(g.get("vega"))
        ts  = q.get("trade_date") or q.get("updated") or now_utc_iso()

        # fallbacks
        fb = chain_fallback_all.get(m["contract"])
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            if fb:
                bid = fb.get("bid") if bid is None else bid
                ask = fb.get("ask") if ask is None else ask
                oi  = fb.get("oi")  if oi  is None else oi
                vol = fb.get("volume") if vol is None else vol
        if fb:
            iv    = iv    if iv    is not None else fb.get("iv")
            delta = delta if delta is not None else fb.get("delta")
            gamma = gamma if gamma is not None else fb.get("gamma")
            theta = theta if theta is not None else fb.get("theta")
            vega  = vega  if vega  is not None else fb.get("vega")
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            lg = last_good.get(m["contract"])
            if isinstance(lg, dict):
                bid = lg.get("bid") if bid is None else bid
                ask = lg.get("ask") if ask is None else ask
                oi  = lg.get("oi")  if oi  is None else oi
                vol = lg.get("volume") if vol is None else vol
                ts  = lg.get("quote_time_utc") or ts

        if (bid is not None) and (ask is not None) and (oi is not None) and (vol is not None):
            last_good[m["contract"]] = {"bid": float(bid), "ask": float(ask), "oi": int(oi), "volume": int(vol),
                                        "iv": float(iv) if iv is not None else None, "quote_time_utc": ts}

        quotes.append({
            "u": m["u"], "contract": m["contract"], "expiry": m["expiry"], "type": m["type"], "strike": m["strike"],
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "last": float(last) if last is not None else None,
            "spread_pct": spread_pct(bid, ask),
            "iv": float(iv) if iv is not None else None,
            "delta": delta, "gamma": gamma, "theta": theta, "vega": vega,
            "oi": int(oi) if oi is not None else None,
            "volume": int(vol) if vol is not None else None,
            "quote_time_utc": ts
        })

    # ATR cache updates
    if atr_updates:
        prev_atr.update(atr_updates)

    # EMA (30-day proxy via EMA of today's chain notional)
    ema_out = {}
    for cm in chain_metrics:
        sym = cm["u"]; x = cm.get("chain_notional_today", 0.0) or 0.0
        prev = (prev_state.get("ema", {}) or {}).get(sym, {}).get("ema30") if isinstance(prev_state, dict) else None
        ema_out[sym] = {"ema30": ema_update(prev, x, alpha=0.15), "last_date": now.date().isoformat()}

    # Coverage stats
    with_delta = sum(1 for q in quotes if q.get("delta") is not None)
    with_core  = sum(1 for q in quotes if all(q.get(k) is not None for k in ("bid","ask","oi","volume")))
    total_q    = len(quotes)
    print(f"[relay] quotes_out={total_q} greeks_with_delta={with_delta} core={with_core}")

    market_payload = {
        "meta": {
            "source": "tradier",
            "updated_utc": now_utc_iso(),
            "interval_sec": INTERVAL_SEC,
            "greeks_coverage": {"with_delta": with_delta, "total": total_q,
                                "ratio": round(with_delta/total_q, 3) if total_q else None},
            "quotes_core_coverage": {"with_core": with_core, "total": total_q,
                                     "ratio": round(with_core/total_q, 3) if total_q else None}
        },
        "underliers": underliers,
        "quotes": quotes,
        "chain_metrics": chain_metrics,
        "state": {
            "ema": ema_out,
            "last_good_quotes": last_good,
            "atr_cache": prev_atr
        }
    }
    gh_put(GIST_ID_MARKET, "market.json", market_payload)

if __name__ == "__main__":
    main()
