#!/usr/bin/env python3
# Publishes to gists:
#   - calendar.json  (Tier-B earnings + macro with IST time)
#   - market.json    (underliers + selected option quotes + chain metrics + EMA + caches)
#   - history.json   (12–20 realized earnings comps per Tier-B symbol)
# Designed for GitHub Actions cron: fast, resilient, and light on API calls.

import os, json, re, urllib.parse
import datetime as dt
from datetime import timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ── ENV ──────────────────────────────────────────────────────────────────────
TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN")
TRADIER_BASE  = os.environ.get("TRADIER_BASE", "https://api.tradier.com/v1")
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN")
GIST_TOKEN    = os.environ.get("GIST_TOKEN")
GIST_ID_CAL   = os.environ.get("GIST_ID_CALENDAR")
GIST_ID_MKT   = os.environ.get("GIST_ID_MARKET")
GIST_ID_HIST  = os.environ.get("GIST_ID_HISTORY")
INTERVAL_SEC  = int(os.environ.get("INTERVAL_SEC", "300"))   # tag only
RELAY_WORKERS = int(os.environ.get("RELAY_WORKERS", "8"))    # thread pool size
TIERB_LIMIT   = int(os.environ.get("TIERB_LIMIT", "0"))      # 0 = no cap

assert TRADIER_TOKEN and TRADIER_BASE and FINNHUB_TOKEN and GIST_TOKEN and GIST_ID_CAL and GIST_ID_MKT and GIST_ID_HIST, "Missing env vars."

# ── HEADERS / TZ ─────────────────────────────────────────────────────────────
HDR_TR = {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept":"application/json"}
HDR_GH = {"Authorization": f"Bearer {GIST_TOKEN}",   "Accept":"application/vnd.github+json"}

IST = dt.timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

# ── UNIVERSE ─────────────────────────────────────────────────────────────────
TIER_A = ["SPX","SPY","QQQ","TSLA","NVDA","AAPL","MSFT","META","AMD","GOOGL","NFLX","AVGO","IWM","SMH","XBI","TLT","GDX"]

# ── HELPERS ──────────────────────────────────────────────────────────────────
def now_utc_iso():
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat()

def to_ist_iso(ts):
    d = dt.datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts, str) else ts
    return d.astimezone(IST).replace(microsecond=0).isoformat()

def http_get_json(url, headers=None, params=None, timeout=20):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def gh_put(gist_id, filename, obj):
    url = f"https://api.github.com/gists/{gist_id}"
    payload = {"files": {filename: {"content": json.dumps(obj, indent=2)}}}  # pretty-print for viewer tools
    r = requests.patch(url, headers=HDR_GH, json=payload, timeout=20)
    r.raise_for_status()

def gh_get(gist_id, filename):
    """Read a single file's JSON content from a gist; return parsed obj or None."""
    try:
        url = f"https://api.github.com/gists/{gist_id}"
        r = requests.get(url, headers=HDR_GH, timeout=20)
        r.raise_for_status()
        j = r.json()
        f = (j.get("files") or {}).get(filename)
        if not f or "content" not in f:
            return None
        return json.loads(f["content"])
    except Exception:
        return None

# ── FINNHUB ──────────────────────────────────────────────────────────────────
def fetch_earnings(from_date, to_date):
    j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                      params={"from": from_date, "to": to_date, "token": FINNHUB_TOKEN})
    out = []
    for x in (j.get("earningsCalendar") or []):
        sym = (x.get("symbol") or "").upper()
        if sym and x.get("date"):
            when = (x.get("hour") or x.get("time") or "").upper()
            out.append({"symbol": sym, "date": x["date"], "when": when})
    return out

def fetch_macro(from_date, to_date):
    # Free plans may 403; fail-open to [] so relay keeps working.
    try:
        j = http_get_json("https://finnhub.io/api/v1/calendar/economic",
                          params={"from": from_date, "to": to_date, "token": FINNHUB_TOKEN})
        arr = (j.get("economicCalendar") or [])
    except Exception:
        arr = []
    out = []
    for x in arr:
        name = (x.get("event") or "").upper()
        if any(k in name for k in ["CPI","PCE","NFP","PAYROLL","FOMC","FED","JOBS"]):
            ts = x.get("time") or x.get("datetime") or (x.get("date")+"T13:30:00Z" if x.get("date") else None)
            if ts:
                out.append({"event": name, "utc_time": dt.datetime.fromisoformat(ts.replace("Z","+00:00")).replace(microsecond=0).isoformat()})
    return out

def fh_earnings_history(symbol, max_quarters=20):
    """Return most-recent earnings dates (YYYY-MM-DD), newest first."""
    dates = []
    try:
        j = http_get_json("https://finnhub.io/api/v1/stock/earnings",
                          params={"symbol": symbol, "token": FINNHUB_TOKEN})
        for r in (j or []):
            d = r.get("date") or r.get("period")
            if d:
                dates.append(str(d)[:10])
    except Exception:
        dates = []
    if not dates:
        try:
            end = dt.datetime.now(UTC).date()
            start = (end - timedelta(days=3*365))
            j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                              params={"from": start.isoformat(), "to": end.isoformat(), "token": FINNHUB_TOKEN})
            for x in (j.get("earningsCalendar") or []):
                if (x.get("symbol") or "").upper() == symbol and x.get("date"):
                    dates.append(x["date"])
        except Exception:
            pass
    dates = sorted(set(dates), reverse=True)[:max_quarters]
    return dates

def fh_when_for_date(symbol, date_str):
    """Return 'AMC'/'BMO'/'' for a specific date; single-day lookup."""
    try:
        j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                          params={"from": date_str, "to": date_str, "token": FINNHUB_TOKEN})
        for x in (j.get("earningsCalendar") or []):
            if (x.get("symbol") or "").upper() == symbol:
                w = (x.get("hour") or x.get("time") or "").upper()
                return "AMC" if "AMC" in w else ("BMO" if "BMO" in w else w)
    except Exception:
        pass
    return ""

# ── TRADIER (prices & chains) ────────────────────────────────────────────────
def quote_underlier(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/quotes", headers=HDR_TR, params={"symbols": sym})
    q = (j.get("quotes") or {}).get("quote")
    if not q: return None
    if isinstance(q, list): q = q[0]
    last = q.get("last") or q.get("close")
    if not last and q.get("bid") and q.get("ask"):
        last = (float(q["bid"]) + float(q["ask"])) / 2
    return float(last) if last else None

def daily_history(sym, start):
    j = http_get_json(f"{TRADIER_BASE}/markets/history", headers=HDR_TR,
                      params={"symbol": sym, "interval": "daily", "start": start})
    days = (j.get("history") or {}).get("day") or []
    return [days] if isinstance(days, dict) else days

def atr_from_history(days, period):
    if len(days) < period + 1: return None
    trs = []
    prev = float(days[0]["close"])
    for d in days[1:]:
        h = float(d["high"]); l = float(d["low"]); c = float(d["close"])
        trs.append(max(h - l, abs(h - prev), abs(l - prev)))
        prev = c
    return sum(trs[-period:]) / period if len(trs) >= period else None

def expirations(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/expirations", headers=HDR_TR,
                      params={"symbol": sym, "includeAllRoots": "true", "strikes": "false"})
    exps = (j.get("expirations") or {}).get("date") or []
    return [exps] if isinstance(exps, str) else sorted(exps)

def chains(sym, exp):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/chains", headers=HDR_TR,
                      params={"symbol": sym, "expiration": exp})
    opts = (j.get("options") or {}).get("option") or []
    return [opts] if isinstance(opts, dict) else opts

# ── Robust options quotes batching (URL-length aware + binary split + OCC validation)
_OCC_RE  = re.compile(r'^[A-Z.]{1,6}\d{6}[CP]\d{8}$')
_MAX_URL = 1500  # conservative full-URL length cap

def _is_valid_occ(sym: str) -> bool:
    return bool(sym) and bool(_OCC_RE.match(sym))

def _fetch_quotes_chunk(chunk_syms):
    params = {"symbols": ",".join(chunk_syms), "greeks": "true"}
    j = http_get_json(f"{TRADIER_BASE}/markets/options/quotes", headers=HDR_TR, params=params)
    rows = (j.get("quotes") or {}).get("quote") or []
    return [rows] if isinstance(rows, dict) else rows

def quotes_options(symbols):
    out = []
    if not symbols: return out
    # de-dup & validate
    seen = set(); syms = []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            if _is_valid_occ(s): syms.append(s)

    i = 0
    while i < len(syms):
        # build chunk under URL cap
        chunk = []
        while i < len(syms):
            test = ",".join(chunk + [syms[i]])
            q = urllib.parse.urlencode({"symbols": test, "greeks": "true"})
            if len(f"{TRADIER_BASE}/markets/options/quotes?{q}") > _MAX_URL:
                if not chunk:
                    chunk = [syms[i]]; i += 1
                break
            chunk.append(syms[i]); i += 1

        try:
            out.extend(_fetch_quotes_chunk(chunk))
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (404, 414):
                # binary split; skip singletons still failing
                stack = [chunk]
                while stack:
                    part = stack.pop()
                    if len(part) == 1:
                        try:
                            out.extend(_fetch_quotes_chunk(part))
                        except requests.HTTPError as e2:
                            if getattr(e2.response, "status_code", None) in (404,414,400): continue
                            raise
                    else:
                        mid = len(part)//2
                        stack.append(part[:mid]); stack.append(part[mid:])
                continue
            raise
    return out

# ── Selection helpers ────────────────────────────────────────────────────────
def nearest_friday_on_or_after(date_str):
    y, m, d = map(int, date_str.split("-"))
    base = dt.date(y, m, d)
    add = (4 - base.weekday()) % 7  # 4 = Friday
    return (base + timedelta(days=add)).isoformat()

def pick_expiration(sym, desired):
    exps = expirations(sym)
    if not exps: return None
    for e in exps:
        if e >= desired: return e
    return exps[-1]

def pick_targets(chain, spot, how_many=2):
    """Pick ATM pair + ±how_many wings (default 2 to keep symbol count modest)."""
    calls = [c for c in chain if (c.get("option_type") or "").lower() == "call"]
    puts  = [p for p in chain if (p.get("option_type") or "").lower() == "put"]
    calls.sort(key=lambda x: abs(float(x.get("strike", 0)) - spot))
    puts.sort(key=lambda x: abs(float(x.get("strike", 0)) - spot))
    best = None; bestd = 1e12
    for c in calls[:how_many*3]:
        cs = float(c.get("strike", 0))
        if not puts: break
        pm = min(puts[:how_many*3], key=lambda p: abs(float(p.get("strike", 0)) - cs))
        d  = abs(cs - spot) + abs(float(pm.get("strike", 0)) - spot)
        if pm and d < bestd:
            bestd = d; best = (c, pm)
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

# ── Realized comps (from Tradier daily bars + Finnhub dates) ────────────────
def _index_daily_by_date(days):
    idx = {}
    for d in days:
        idx[str(d["date"])] = {
            "o": float(d["open"]), "h": float(d["high"]),
            "l": float(d["low"]),  "c": float(d["close"])
        }
    return idx

def compute_comps(symbol, earn_dates, when_map=None):
    """
    For each earnings date:
      AMC -> gap% = |Open_{D+1} - Close_{D0}| / Close_{D0}; range% = (High_{D+1} - Low_{D+1}) / Close_{D0}
      BMO -> gap% = |Open_{D0} - Close_{D-1}| / Close_{D-1}; range% = (High_{D0} - Low_{D0}) / Close_{D-1}
    Skips entries when any required bar is missing.
    """
    start = (dt.datetime.now(UTC) - timedelta(days=3*365)).date().isoformat()
    days  = daily_history(symbol, start)
    if not days: return []
    idx = _index_daily_by_date(days)
    out = []

    for d in earn_dates:
        w = (when_map or {}).get(d, "") or fh_when_for_date(symbol, d) or ""
        d0 = dt.date.fromisoformat(d)

        # closest available D-1 and D+1 within ±3 days
        d_1 = None; d1 = None
        for k in range(1, 4):
            cand = (d0 - timedelta(days=k)).isoformat()
            if cand in idx: d_1 = cand; break
        for k in range(1, 4):
            cand = (d0 + timedelta(days=k)).isoformat()
            if cand in idx: d1 = cand; break

        bar_d0 = idx.get(d)
        if w == "AMC":
            if not (bar_d0 and d1 and (d in idx) and (d1 in idx)): continue
            c0 = idx[d]["c"]; o1 = idx[d1]["o"]; h1 = idx[d1]["h"]; l1 = idx[d1]["l"]
            gap = abs(o1 - c0)/c0
            rng = (h1 - l1)/c0
        else:  # default to BMO (or unknown)
            if not (d_1 and (d_1 in idx) and bar_d0): continue
            c_1 = idx[d_1]["c"]; o0 = bar_d0["o"]; h0 = bar_d0["h"]; l0 = bar_d0["l"]
            gap = abs(o0 - c_1)/c_1
            rng = (h0 - l0)/c_1

        out.append({"date": d, "when": (w or "BMO"), "gap_pct": round(gap, 6), "range_pct": round(rng, 6)})
    return out

def build_history_for_symbols(symbols, max_quarters=20):
    """Update history.json with comps for the provided symbols if stale (>7 days) or missing."""
    hist = gh_get(GIST_ID_HIST, "history.json") or {"meta":{}, "symbols":{}}
    symmap = hist.setdefault("symbols", {})
    changed = False

    for sym in symbols:
        s = sym.upper()
        try:
            last_ts = (symmap.get(s) or {}).get("_updated_utc")
            stale = True
            if last_ts:
                try:
                    dt_last = dt.datetime.fromisoformat(last_ts.replace("Z","+00:00"))
                    stale = (dt.datetime.now(UTC) - dt_last).days >= 7
                except Exception:
                    stale = True
            if not stale: continue

            dates = fh_earnings_history(s, max_quarters=max_quarters)
            if not dates: continue
            comps = compute_comps(s, dates, when_map={})
            if comps:
                symmap[s] = {"_updated_utc": now_utc_iso(), "comps": comps}
                changed = True
        except Exception:
            continue

    if changed:
        hist["meta"] = {"updated_utc": now_utc_iso(), "lookback_years": 3}
        gh_put(GIST_ID_HIST, "history.json", hist)

# ── Chain metrics + EMA ──────────────────────────────────────────────────────
def chain_metrics_from_chain(chain):
    """
    Compute chain-wide:
      - chain_oi_total (sum open_interest)
      - chain_median_spread_pct (median (ask-bid)/mid, where bid/ask>0)
      - chain_notional_today (sum volume * mid * 100)
    Using only fields present on /markets/options/chains rows.
    """
    oi_sum = 0
    spreads = []
    notional = 0.0
    for o in chain:
        try:
            oi  = int(o.get("open_interest") or 0)
            vol = int(o.get("volume") or 0)
            bid = float(o.get("bid")) if o.get("bid") is not None else None
            ask = float(o.get("ask")) if o.get("ask") is not None else None
            oi_sum += oi
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    spreads.append((ask - bid) / mid)
                    if vol > 0:
                        notional += vol * mid * 100.0
        except Exception:
            continue
    spreads.sort()
    med_spread = spreads[len(spreads)//2] if spreads else None
    return {"chain_oi_total": oi_sum, "chain_median_spread_pct": med_spread, "chain_notional_today": notional}

def ema_update(prev, x, alpha=0.15):
    return x if prev is None else (alpha * x + (1 - alpha) * prev)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    now = dt.datetime.now(UTC)
    from_date = now.date().isoformat()
    to_date   = (now + timedelta(days=14)).date().isoformat()

    # Calendars
    earn  = fetch_earnings(from_date, to_date)
    macro = fetch_macro(from_date, (now + timedelta(days=7)).date().isoformat())

    # Tier-B symbols; optional cap (keep earliest by date)
    if TIERB_LIMIT and TIERB_LIMIT > 0 and len(earn) > TIERB_LIMIT:
        earn_sorted = sorted(earn, key=lambda x: x["date"])
        tierB_syms = [x["symbol"] for x in earn_sorted[:TIERB_LIMIT]]
    else:
        tierB_syms = sorted({e["symbol"] for e in earn})
    symbols = sorted(set(TIER_A + tierB_syms))

    # Build/refresh realized comps for Tier-B only (very light)
    if tierB_syms:
        build_history_for_symbols(tierB_syms, max_quarters=20)

    # Publish calendar.json
    cal_payload = {
        "meta":  {"source":"finnhub","updated_utc": now_utc_iso(), "window_from": from_date, "window_to": to_date},
        "tierA": TIER_A,
        "tierB": earn,
        "macro":[{**m, "ist_time": to_ist_iso(m["utc_time"])} for m in macro]
    }
    gh_put(GIST_ID_CAL, "calendar.json", cal_payload)

    # Load previous market for state (EMA, last_good, ATR cache)
    prev_market    = gh_get(GIST_ID_MKT, "market.json") or {}
    prev_state     = prev_market.get("state", {}) if isinstance(prev_market, dict) else {}
    prev_ema       = prev_state.get("ema", {}) if isinstance(prev_state, dict) else {}
    prev_last_good = prev_state.get("last_good_quotes", {}) if isinstance(prev_state, dict) else {}
    prev_atr       = prev_state.get("atr_cache", {}) if isinstance(prev_state, dict) else {}

    # Collections to fill
    underliers, opt_symbols, sel_meta, chain_metrics = [], [], [], []
    chain_index_fallback_all = {}
    atr_updates = {}
    hist_start = (now - timedelta(days=120)).date().isoformat()
    today_date = dt.datetime.now(UTC).date().isoformat()

    # Per-symbol worker (spot, ATR (with cache), expiry, chain, metrics, legs, fallbacks)
    def process_symbol(u):
        try:
            spot = quote_underlier(u)
            if not spot:
                return None

            # ATR from cache if same day; else compute and return update
            atr5 = atr14 = None
            cache = prev_atr.get(u)
            if cache and cache.get("asof") == today_date:
                atr5, atr14 = cache.get("atr5"), cache.get("atr14")
            else:
                days = daily_history(u, hist_start)
                atr5  = atr_from_history(days[-40:], 5) if days else None
                atr14 = atr_from_history(days[-80:],14) if days else None
                atr_updates[u] = {"asof": today_date, "atr5": atr5, "atr14": atr14}

            # choose expiry
            e_dates = [x["date"] for x in earn if x["symbol"]==u]
            desired = nearest_friday_on_or_after(e_dates[0]) if e_dates else nearest_friday_on_or_after((now + timedelta(days=7)).date().isoformat())
            expiry  = pick_expiration(u, desired)
            if not expiry:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}, "cm": None, "legs": [], "fallback": {}}

            chain = chains(u, expiry)
            if not chain:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}, "cm": None, "legs": [], "fallback": {}}

            cm = chain_metrics_from_chain(chain)

            # Pre-filter: only fetch quotes for chains that pass hard gates
            pass_chain = True
            if cm.get("chain_oi_total", 0) < 15000:
                pass_chain = False
            ms = cm.get("chain_median_spread_pct")
            if ms is not None and ms > 0.10:
                pass_chain = False

            legs = []
            fallback = {}
            if pass_chain:
                picks = pick_targets(chain, spot, how_many=2)
                if picks["atm"]:
                    legs += list(picks["atm"])
                for c_leg, p_leg in picks["wings"]:
                    legs += [c_leg, p_leg]
                # chain fallback map for just the selected legs
                leg_syms = {l.get("symbol") for l in legs if l.get("symbol")}
                for o in chain:
                    if o.get("symbol") in leg_syms:
                        try:
                            fallback[o["symbol"]] = {
                                "bid": float(o.get("bid")) if o.get("bid") is not None else None,
                                "ask": float(o.get("ask")) if o.get("ask") is not None else None,
                                "volume": int(o.get("volume")) if o.get("volume") is not None else None,
                                "oi": int(o.get("open_interest")) if o.get("open_interest") is not None else None
                            }
                        except Exception:
                            pass

            return {
                "underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()},
                "cm": {"u": u, "expiry": expiry, **cm},
                "legs": legs,
                "fallback": fallback
            }
        except Exception:
            return None

    # Run workers in parallel
    results = []
    with ThreadPoolExecutor(max_workers=RELAY_WORKERS) as ex:
        futures = {ex.submit(process_symbol, u): u for u in symbols}
        for fut in as_completed(futures):
            r = fut.result()
            if r: results.append(r)

    # Aggregate results
    for r in results:
        if r.get("underlier"): underliers.append(r["underlier"])
        if r.get("cm"):        chain_metrics.append(r["cm"])
        if r.get("fallback"):  chain_index_fallback_all.update(r["fallback"])
        for leg in r.get("legs", []):
            sym = leg.get("symbol")
            if sym:
                opt_symbols.append(sym)
                sel_meta.append({
                    "u": r["cm"]["u"],
                    "expiry": r["cm"]["expiry"],
                    "type": ("C" if leg["option_type"]=="call" else "P"),
                    "strike": float(leg["strike"]),
                    "contract": sym
                })

    # Fetch quotes for selected legs (robust batching)
    qts = quotes_options(opt_symbols)
    by_sym = {q.get("symbol"): q for q in qts} if qts else {}

    # Build quotes with fallbacks (quotes -> chain -> last_good)
    last_good = dict(prev_last_good)  # will update and persist
    quotes = []
    for m in sel_meta:
        q   = by_sym.get(m["contract"], {})
        bid = q.get("bid"); ask = q.get("ask"); last = q.get("last")
        oi  = q.get("open_interest"); vol = q.get("volume")
        iv  = None
        if q.get("greeks"):
            iv = q["greeks"].get("mid_iv") or q["greeks"].get("smv_vol")
        ts  = q.get("trade_date") or q.get("updated") or now_utc_iso()

        # 1) fallback to chain() if missing
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            fb = chain_index_fallback_all.get(m["contract"])
            if fb:
                bid = fb["bid"]     if bid is None else bid
                ask = fb["ask"]     if ask is None else ask
                oi  = fb["oi"]      if oi  is None else oi
                vol = fb["volume"]  if vol is None else vol

        # 2) fallback to last_good cache if still missing
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            lg = prev_last_good.get(m["contract"])
            if isinstance(lg, dict):
                bid = lg.get("bid")     if bid is None else bid
                ask = lg.get("ask")     if ask is None else ask
                oi  = lg.get("oi")      if oi  is None else oi
                vol = lg.get("volume")  if vol is None else vol
                ts  = lg.get("quote_time_utc") or ts

        # Update last_good if we now have all key fields
        if (bid is not None) and (ask is not None) and (oi is not None) and (vol is not None):
            last_good[m["contract"]] = {
                "bid": float(bid), "ask": float(ask), "oi": int(oi), "volume": int(vol),
                "iv": float(iv) if iv is not None else None,
                "quote_time_utc": ts
            }

        quotes.append({
            "u": m["u"], "contract": m["contract"], "expiry": m["expiry"], "type": m["type"], "strike": m["strike"],
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "last": float(last) if last is not None else None,
            "spread_pct": spread_pct(bid, ask),
            "iv": float(iv) if iv is not None else None,
            "oi": int(oi) if oi is not None else None,
            "volume": int(vol) if vol is not None else None,
            "quote_time_utc": ts
        })

    # Update ATR cache with thread results
    if atr_updates:
        prev_atr.update(atr_updates)

    # Update EMA state from today's chain_notional
    ema_out = {}
    for cm in chain_metrics:
        sym = cm["u"]
        x   = cm.get("chain_notional_today", 0.0) or 0.0
        prev = None
        if isinstance(prev_ema.get(sym), dict):
            prev = prev_ema[sym].get("ema30")
        ema_val = ema_update(prev, x, alpha=0.15)
        ema_out[sym] = {"ema30": ema_val, "last_date": today_date}

    # Publish market.json
    market_payload = {
        "meta": {"source": "tradier", "updated_utc": now_utc_iso(), "interval_sec": INTERVAL_SEC},
        "underliers": underliers,
        "quotes": quotes,
        "chain_metrics": chain_metrics,
        "state": {
            "ema": ema_out,
            "last_good_quotes": last_good,
            "atr_cache": prev_atr
        }
    }
    gh_put(GIST_ID_MKT, "market.json", market_payload)

if __name__ == "__main__":
    main()
