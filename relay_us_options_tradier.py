#!/usr/bin/env python3
# Publishes to your gists:
#   - calendar.json  (Tier-B earnings [optional via Finnhub] + MACRO from official sources + IST time + state.macro_cache)
#   - market.json    (underliers + selected option quotes + greeks + chain metrics + EMA + caches + coverage stats)
#   - history.json   (12–20 realized earnings comps per Tier-B symbol)
# Designed for GitHub Actions cron: fast, resilient, and light on API calls.

import os, json, re, urllib.parse, html
import datetime as dt
from datetime import timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ── ENV ──────────────────────────────────────────────────────────────────────
TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN")
TRADIER_BASE  = os.environ.get("TRADIER_BASE", "https://api.tradier.com/v1")
# Finnhub for earnings is optional; macro is now official-free only
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")
GIST_TOKEN    = os.environ.get("GIST_TOKEN")
GIST_ID_CAL   = os.environ.get("GIST_ID_CALENDAR")
GIST_ID_MKT   = os.environ.get("GIST_ID_MARKET")
GIST_ID_HIST  = os.environ.get("GIST_ID_HISTORY")

INTERVAL_SEC      = int(os.environ.get("INTERVAL_SEC", "300"))   # tag only
RELAY_WORKERS     = int(os.environ.get("RELAY_WORKERS", "8"))    # thread pool size
TIERB_LIMIT       = int(os.environ.get("TIERB_LIMIT", "0"))      # 0 = no cap
MACRO_TTL_HOURS   = int(os.environ.get("MACRO_TTL_HOURS", "12"))
MACRO_FORCE_REFRESH = os.environ.get("MACRO_FORCE_REFRESH", "").strip().lower() in ("1","true","yes")
MACRO_WINDOW_DAYS = int(os.environ.get("MACRO_WINDOW_DAYS", "21"))  # look-ahead window for macro

assert all([TRADIER_TOKEN, TRADIER_BASE, GIST_TOKEN, GIST_ID_CAL, GIST_ID_MKT, GIST_ID_HIST]), "Missing env vars."

# ── HEADERS / TZ ─────────────────────────────────────────────────────────────
HDR_TR = {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept":"application/json"}
HDR_GH = {"Authorization": f"Bearer {GIST_TOKEN}",   "Accept":"application/vnd.github+json"}

IST = dt.timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

# ── UNIVERSE ─────────────────────────────────────────────────────────────────
TIER_A = [
    "SPX","XSP","SPY","QQQ","TSLA","NVDA","AAPL","MSFT","META",
    "AMD","GOOGL","NFLX","AVGO","IWM","SMH","XBI","TLT","GDX"
]

# ── HELPERS ──────────────────────────────────────────────────────────────────
def now_utc_iso():
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat()

def to_ist_iso(ts):
    d = dt.datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts, str) else ts
    return d.astimezone(IST).replace(microsecond=0).isoformat()

def http_get_text(url, headers=None, params=None, timeout=25):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def http_get_json(url, headers=None, params=None, timeout=25):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def gh_put(gist_id, filename, obj):
    url = f"https://api.github.com/gists/{gist_id}"
    payload = {"files": {filename: {"content": json.dumps(obj, indent=2)}}}  # pretty-print for raw viewer tools
    r = requests.patch(url, headers=HDR_GH, json=payload, timeout=25)
    r.raise_for_status()

def gh_get(gist_id, filename):
    """Read a single file's JSON content from a gist; return parsed obj or None."""
    try:
        url = f"https://api.github.com/gists/{gist_id}"
        r = requests.get(url, headers=HDR_GH, timeout=25)
        r.raise_for_status()
        j = r.json()
        f = (j.get("files") or {}).get(filename)
        if not f or "content" not in f:
            return None
        return json.loads(f["content"])
    except Exception:
        return None

# ---- Macro cache helpers ----
def _covers_window(cache_from, cache_to, req_from, req_to):
    try:
        cf = dt.date.fromisoformat(cache_from); ct = dt.date.fromisoformat(cache_to)
        rf = dt.date.fromisoformat(req_from);   rt = dt.date.fromisoformat(req_to)
        return cf <= rf and ct >= rt
    except Exception:
        return False

def _age_hours(ts_iso):
    try:
        t = dt.datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        return (dt.datetime.now(UTC) - t).total_seconds()/3600.0
    except Exception:
        return 1e9

def _dedupe_macro(evts):
    seen=set(); out=[]
    for e in evts or []:
        key=(e.get("event",""), e.get("utc_time",""))
        if key in seen: continue
        seen.add(key); out.append(e)
    return out

def _within_window(date_iso, req_from, req_to):
    try:
        d = dt.date.fromisoformat(date_iso)
        return dt.date.fromisoformat(req_from) <= d <= dt.date.fromisoformat(req_to)
    except Exception:
        return False

def _to_utc_iso(date_obj, hour=13, minute=30):
    # default 13:30 UTC for 8:30 ET prints; for FOMC we'll use 18:00 UTC
    return dt.datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, tzinfo=UTC).isoformat()

# ── OFFICIAL MACRO FETCHERS (free) ───────────────────────────────────────────
_MONTHS = "(January|February|March|April|May|June|July|August|September|October|November|December)"
DATE_RE = re.compile(rf"{_MONTHS}\s+\d{{1,2}},\s+\d{{4}}", re.I)

def _parse_dates_from_html(text):
    """Extract 'Month DD, YYYY' dates from an HTML page."""
    text = html.unescape(text)
    return [m.group(0) for m in DATE_RE.finditer(text)]

def _to_date(s):
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try: return dt.datetime.strptime(s, fmt).date()
        except Exception: pass
    return None

def fetch_bls_cpi(from_date, to_date):
    # BLS CPI release schedule: https://www.bls.gov/schedule/news_release/cpi.htm
    url = "https://www.bls.gov/schedule/news_release/cpi.htm"
    try:
        txt = http_get_text(url)
    except Exception:
        return []
    dates = []
    for s in _parse_dates_from_html(txt):
        d = _to_date(s)
        if d and _within_window(d.isoformat(), from_date, to_date):
            dates.append(d)
    # Deduce uniqueness and map to event with 13:30 UTC default (8:30 ET)
    out = [{"event":"CPI", "utc_time": _to_utc_iso(d, 13, 30)} for d in sorted(set(dates))]
    return out

def fetch_bls_nfp(from_date, to_date):
    # BLS Employment Situation (NFP) schedule: https://www.bls.gov/schedule/news_release/empsit.htm
    url = "https://www.bls.gov/schedule/news_release/empsit.htm"
    try:
        txt = http_get_text(url)
    except Exception:
        return []
    dates = []
    for s in _parse_dates_from_html(txt):
        d = _to_date(s)
        if d and _within_window(d.isoformat(), from_date, to_date):
            dates.append(d)
    out = [{"event":"NFP", "utc_time": _to_utc_iso(d, 13, 30)} for d in sorted(set(dates))]
    return out

def fetch_bea_pce(from_date, to_date):
    # BEA schedule page contains "Personal Income and Outlays" dates: https://www.bea.gov/news/schedule
    url = "https://www.bea.gov/news/schedule"
    try:
        txt = http_get_text(url)
    except Exception:
        return []
    # Keep only date strings near "Personal Income and Outlays" or "PCE"
    window_events = []
    # Quick heuristic: for each match, ensure nearby context mentions "Personal Income"
    for m in DATE_RE.finditer(txt):
        start = max(0, m.start()-200)
        end   = min(len(txt), m.end()+200)
        ctx   = txt[start:end].lower()
        if "personal income and outlays" in ctx or "pce" in ctx:
            d = _to_date(m.group(0))
            if d and _within_window(d.isoformat(), from_date, to_date):
                window_events.append(d)
    out = [{"event":"PCE", "utc_time": _to_utc_iso(d, 13, 30)} for d in sorted(set(window_events))]
    return out

def fetch_fomc(from_date, to_date):
    # Fed FOMC calendar page: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    try:
        txt = http_get_text(url)
    except Exception:
        return []
    # FOMC meetings often span two days; statement typically on last day at 2:00 PM ET.
    # We'll capture all dates on the page and treat those within window as statement day.
    dates = []
    for s in _parse_dates_from_html(txt):
        d = _to_date(s)
        if d and _within_window(d.isoformat(), from_date, to_date):
            dates.append(d)
    # Deduplicate and tag at 18:00 UTC (approx 2:00 PM ET; downstream scanner treats as macro day)
    out = [{"event":"FOMC", "utc_time": _to_utc_iso(d, 18, 0)} for d in sorted(set(dates))]
    return out

def fetch_macro_live_official(from_date, to_date):
    """Aggregate CPI (BLS), NFP (BLS), PCE (BEA), FOMC (Fed) for the window; merge + dedupe."""
    ev = []
    ev += fetch_bls_cpi(from_date, to_date)
    ev += fetch_bls_nfp(from_date, to_date)
    ev += fetch_bea_pce(from_date, to_date)
    ev += fetch_fomc(from_date, to_date)
    return _dedupe_macro(ev)

def fetch_macro_cached(req_from, req_to):
    """12h macro cache in calendar.json.state.macro_cache. Never wipe a good cache with empties."""
    prev_cal = gh_get(GIST_ID_CAL, "calendar.json") or {}
    state    = prev_cal.get("state", {}) if isinstance(prev_cal, dict) else {}
    cache    = state.get("macro_cache", {}) if isinstance(state, dict) else {}

    cached_events = cache.get("events") or []
    cached_from   = cache.get("from")
    cached_to     = cache.get("to")
    cached_ts     = cache.get("updated_utc")

    covers = cached_events and cached_from and cached_to and _covers_window(cached_from, cached_to, req_from, req_to)
    fresh  = cached_ts and _age_hours(cached_ts) < MACRO_TTL_HOURS

    if covers and fresh and not MACRO_FORCE_REFRESH:
        return {"events": cached_events, "source": cache.get("source","cache"), "used_cache": True, "cache": cache}

    # live official fetch
    evts = fetch_macro_live_official(req_from, req_to)
    source = "official-aggregate"

    if evts:
        new_cache = {"from": req_from, "to": req_to, "updated_utc": now_utc_iso(),
                     "source": source, "events": evts}
        return {"events": evts, "source": source, "used_cache": False, "cache": new_cache}

    # if live failed, fall back to stale cache (don't clear)
    if cached_events:
        return {"events": cached_events, "source": "stale-cache", "used_cache": True, "cache": cache}

    return {"events": [], "source": "none", "used_cache": False, "cache": {}}

# ── FINNHUB EARNINGS (optional) ──────────────────────────────────────────────
def fetch_earnings(from_date, to_date):
    if not FINNHUB_TOKEN:
        return []
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
    # include greeks=true so ORATS greeks + IV are present on each chain row
    j = http_get_json(f"{TRADIER_BASE}/markets/options/chains", headers=HDR_TR,
                      params={"symbol": sym, "expiration": exp, "greeks": "true"})
    opts = (j.get("options") or {}).get("option") or []
    return [opts] if isinstance(opts, dict) else opts

# ── Robust options quotes batching (URL-length aware + binary split + OCC validation)
_OCC_RE  = re.compile(r'^[A-Z.]{1,6}\d{6}[CP]\d{8}$')
_OCC_TYPE_RE = re.compile(r'\d{6}([CP])\d{8}$')
_OCC_STRIKE_RE = re.compile(r'(\d{8})$')
_MAX_URL = 1500  # conservative full-URL length cap

def _is_valid_occ(sym: str) -> bool:
    return bool(sym) and bool(_OCC_RE.match(sym))

def _occ_type(sym: str):
    m = _OCC_TYPE_RE.search(sym or "")
    if not m: return None
    return "call" if m.group(1) == "C" else "put"

def _occ_strike(sym: str):
    m = _OCC_STRIKE_RE.search(sym or "")
    if not m: return None
    try: return int(m.group(1))/1000.0
    except: return None

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

# ── Realized comps (Tradier daily bars + Finnhub dates) ─────────────────────
def _index_daily_by_date(days):
    idx = {}
    for d in days:
        idx[str(d["date"])] = {
            "o": float(d["open"]), "h": float(d["high"]),
            "l": float(d["low"]),  "c": float(d["close"])
        }
    return idx

def fh_earnings_history(symbol, max_quarters=20):
    if not FINNHUB_TOKEN:
        return []
    dates = []
    try:
        j = http_get_json("https://finnhub.io/api/v1/stock/earnings",
                          params={"symbol": symbol, "token": FINNHUB_TOKEN})
        for r in (j or []):
            d = r.get("date") or r.get("period")
            if d: dates.append(str(d)[:10])
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
    if not FINNHUB_TOKEN:
        return ""
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

def compute_comps(symbol, earn_dates, when_map=None):
    start = (dt.datetime.now(UTC) - timedelta(days=3*365)).date().isoformat()
    days  = daily_history(symbol, start)
    if not days: return []
    idx = _index_daily_by_date(days)
    out = []
    for d in earn_dates:
        w = (when_map or {}).get(d, "") or fh_when_for_date(symbol, d) or ""
        d0 = dt.date.fromisoformat(d)
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
        else:
            if not (d_1 and (d_1 in idx) and bar_d0): continue
            c_1 = idx[d_1]["c"]; o0 = bar_d0["o"]; h0 = bar_d0["h"]; l0 = bar_d0["l"]
            gap = abs(o0 - c_1)/c_1
            rng = (h0 - l0)/c_1
        out.append({"date": d, "when": (w or "BMO"), "gap_pct": round(gap, 6), "range_pct": round(rng, 6)})
    return out

def build_history_for_symbols(symbols, max_quarters=20):
    if not FINNHUB_TOKEN:  # no token → skip silently
        return
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

    # Earnings (optional)
    earn  = fetch_earnings(from_date, to_date)

    # Macro (official, cached)
    macro_to = (now + timedelta(days=MACRO_WINDOW_DAYS)).date().isoformat()
    macro_pkg = fetch_macro_cached(from_date, macro_to)
    macro     = macro_pkg["events"]
    print(f"[relay] macro_source={macro_pkg.get('source')} used_cache={macro_pkg.get('used_cache')} count={len(macro)} window={from_date}..{macro_to}")

    # Tier-B symbols; optional cap (keep earliest by date)
    if TIERB_LIMIT and TIERB_LIMIT > 0 and len(earn) > TIERB_LIMIT:
        earn_sorted = sorted(earn, key=lambda x: x["date"])
        tierB_syms = [x["symbol"] for x in earn_sorted[:TIERB_LIMIT]]
    else:
        tierB_syms = sorted({e["symbol"] for e in earn})
    symbols = sorted(set(TIER_A + tierB_syms))
    print(f"[relay] symbols={len(symbols)} tierB={len(tierB_syms)}")

    # Build/refresh realized comps for Tier-B only (if FINNHUB_TOKEN present)
    if tierB_syms and FINNHUB_TOKEN:
        build_history_for_symbols(tierB_syms, max_quarters=20)

    # Publish calendar.json (with macro_cache state)
    cal_payload = {
        "meta":  {
            "source":"official",
            "updated_utc": now_utc_iso(),
            "window_from": from_date,
            "window_to": to_date,
            "macro_source": macro_pkg.get("source","")
        },
        "tierA": TIER_A,
        "tierB": earn,
        "macro":[{**m, "ist_time": to_ist_iso(m["utc_time"])} for m in macro],
        "state": {"macro_cache": macro_pkg.get("cache", {})}
    }
    gh_put(GIST_ID_CAL, "calendar.json", cal_payload)

    # Load previous market state
    prev_market    = gh_get(GIST_ID_MKT, "market.json") or {}
    prev_state     = prev_market.get("state", {}) if isinstance(prev_market, dict) else {}
    prev_ema       = prev_state.get("ema", {}) if isinstance(prev_state, dict) else {}
    prev_last_good = prev_state.get("last_good_quotes", {}) if isinstance(prev_state, dict) else {}
    prev_atr       = prev_state.get("atr_cache", {}) if isinstance(prev_state, dict) else {}

    # Collections
    underliers, opt_symbols, sel_meta, chain_metrics = [], [], [], []
    chain_index_fallback_all = {}
    wing_selection = {}
    atr_updates = {}
    hist_start = (now - timedelta(days=120)).date().isoformat()
    today_date = dt.datetime.now(UTC).date().isoformat()

    # Per-symbol worker
    def process_symbol(u):
        try:
            spot = quote_underlier(u)
            if not spot:
                return None

            # ATR cache
            atr5 = atr14 = None
            cache = prev_atr.get(u)
            if cache and cache.get("asof") == today_date:
                atr5, atr14 = cache.get("atr5"), cache.get("atr14")
            else:
                days = daily_history(u, hist_start)
                atr5  = atr_from_history(days[-40:], 5) if days else None
                atr14 = atr_from_history(days[-80:],14) if days else None
                atr_updates[u] = {"asof": today_date, "atr5": atr5, "atr14": atr14}

            # expiry
            e_dates = [x["date"] for x in earn if x["symbol"]==u]
            desired = nearest_friday_on_or_after(e_dates[0]) if e_dates else nearest_friday_on_or_after((now + timedelta(days=7)).date().isoformat())
            expiry  = pick_expiration(u, desired)
            if not expiry:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}, "cm": None, "legs": [], "fallback": {}, "selection": {}}

            chain = chains(u, expiry)
            if not chain:
                return {"underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()}, "cm": None, "legs": [], "fallback": {}, "selection": {}}

            cm = chain_metrics_from_chain(chain)

            # Chain prefilter gates
            pass_chain = True
            if cm.get("chain_oi_total", 0) < 15000:
                pass_chain = False
            ms = cm.get("chain_median_spread_pct")
            if ms is not None and ms > 0.10:
                pass_chain = False

            legs = []
            fallback = {}
            selection = {}

            if pass_chain:
                # ATM + ±2 strike wings
                picks = pick_targets(chain, spot, how_many=2)
                if picks["atm"]:
                    legs += list(picks["atm"])
                for c_leg, p_leg in picks["wings"]:
                    legs += [c_leg, p_leg]

                # widen pool ±5 per side and select ~15Δ/~20Δ wings using chain greeks
                calls = sorted([c for c in chain if (c.get("option_type") or "").lower()=="call"], key=lambda x: abs(float(x.get("strike",0))-spot))
                puts  = sorted([p for p in chain if (p.get("option_type") or "").lower()=="put"],  key=lambda x: abs(float(x.get("strike",0))-spot))
                pool  = calls[:6] + puts[:6]

                # symbol->greeks (delta) from chain
                def fnum(x):
                    try: return float(x)
                    except: return None
                by_chain = {}
                for o in pool:
                    sym = o.get("symbol")
                    if not sym: continue
                    g = o.get("greeks") or {}
                    by_chain[sym] = {"delta": fnum(g.get("delta"))}

                def best_delta(target, side):  # side: 'call' or 'put'
                    best = None; bd = 1e9
                    for o in pool:
                        typ = (o.get("option_type") or "").lower()
                        if typ != side: continue
                        sym = o.get("symbol")
                        d = by_chain.get(sym,{}).get("delta")
                        if d is None: continue
                        d_abs = abs(d)
                        dd = abs(d_abs - target)
                        if dd < bd:
                            bd = dd; best = o
                    return best

                c15 = best_delta(0.15, "call"); c20 = best_delta(0.20, "call")
                p15 = best_delta(0.15, "put");  p20 = best_delta(0.20, "put")

                def o_to_leg(o):
                    if not o: return None
                    sym = o.get("symbol")
                    if not sym: return None
                    strike = o.get("strike")
                    if strike is None: strike = _occ_strike(sym)
                    try: strike = float(strike) if strike is not None else None
                    except: strike = None
                    return {"symbol": sym, "strike": strike, "option_type": (o.get("option_type") or _occ_type(sym))}

                for tag, row in (("call_15",c15), ("call_20",c20), ("put_15",p15), ("put_20",p20)):
                    leg = o_to_leg(row)
                    selection[tag] = leg["symbol"] if leg else None
                    if leg: legs.append(leg)

                # chain fallback including greeks/iv for selected legs
                leg_syms = {l.get("symbol") for l in legs if l.get("symbol")}
                for o in chain:
                    if o.get("symbol") in leg_syms:
                        g = o.get("greeks") or {}
                        try:
                            fallback[o["symbol"]] = {
                                "bid": fnum(o.get("bid")),
                                "ask": fnum(o.get("ask")),
                                "volume": int(o.get("volume")) if o.get("volume") is not None else None,
                                "oi": int(o.get("open_interest")) if o.get("open_interest") is not None else None,
                                "iv": fnum(g.get("mid_iv")) or fnum(g.get("smv_vol")),
                                "delta": fnum(g.get("delta")),
                                "gamma": fnum(g.get("gamma")),
                                "theta": fnum(g.get("theta")),
                                "vega": fnum(g.get("vega")),
                            }
                        except Exception:
                            pass

            return {
                "underlier": {"u": u, "spot": spot, "atr5": atr5, "atr14": atr14, "updated_utc": now_utc_iso()},
                "cm": {"u": u, "expiry": expiry, **cm},
                "legs": legs,
                "fallback": fallback,
                "selection": selection
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

    # Aggregate
    underliers, opt_symbols, sel_meta, chain_metrics = [], [], [], []
    chain_index_fallback_all = {}
    wing_selection = {}
    for r in results:
        if r.get("underlier"): underliers.append(r["underlier"])
        if r.get("cm"):        chain_metrics.append(r["cm"])
        if r.get("fallback"):  chain_index_fallback_all.update(r["fallback"])
        if r.get("selection"): wing_selection[r["cm"]["u"]] = r["selection"]
        for leg in r.get("legs", []):
            sym = leg.get("symbol")
            if sym:
                strike = leg.get("strike")
                if strike is None: strike = _occ_strike(sym)
                opt_symbols.append(sym)
                sel_meta.append({
                    "u": r["cm"]["u"],
                    "expiry": r["cm"]["expiry"],
                    "type": ("C" if (leg.get("option_type") or _occ_type(sym))=="call" else "P"),
                    "strike": float(strike) if strike is not None else None,
                    "contract": sym
                })

    # Quotes for selected legs (robust batching)
    qts = quotes_options(opt_symbols)
    by_sym = {q.get("symbol"): q for q in qts} if qts else {}

    # Build quotes with fallbacks; persist greeks
    prev_market    = gh_get(GIST_ID_MKT, "market.json") or {}
    prev_state     = prev_market.get("state", {}) if isinstance(prev_market, dict) else {}
    prev_last_good = prev_state.get("last_good_quotes", {}) if isinstance(prev_state, dict) else {}
    prev_ema       = prev_state.get("ema", {}) if isinstance(prev_state, dict) else {}
    prev_atr       = prev_state.get("atr_cache", {}) if isinstance(prev_state, dict) else {}

    last_good = dict(prev_last_good)
    quotes = []
    for m in sel_meta:
        q   = by_sym.get(m["contract"], {})
        bid = q.get("bid"); ask = q.get("ask"); last = q.get("last")
        oi  = q.get("open_interest"); vol = q.get("volume")

        # greeks from quotes
        delta = gamma = theta = vega = None
        iv = None
        if q.get("greeks"):
            g = q["greeks"]
            def f(x):
                try: return float(x)
                except: return None
            iv    = f(g.get("mid_iv")) or f(g.get("smv_vol"))
            delta = f(g.get("delta"))
            gamma = f(g.get("gamma"))
            theta = f(g.get("theta"))
            vega  = f(g.get("vega"))

        ts  = q.get("trade_date") or q.get("updated") or now_utc_iso()

        # 1) fallback to chain() if core tape missing
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            fb = chain_index_fallback_all.get(m["contract"])
            if fb:
                bid = fb.get("bid")     if bid is None else bid
                ask = fb.get("ask")     if ask is None else ask
                oi  = fb.get("oi")      if oi  is None else oi
                vol = fb.get("volume")  if vol is None else vol

        # fill greeks/iv from chain if missing
        fb = chain_index_fallback_all.get(m["contract"])
        if fb:
            iv    = iv    if iv    is not None else fb.get("iv")
            delta = delta if delta is not None else fb.get("delta")
            gamma = gamma if gamma is not None else fb.get("gamma")
            theta = theta if theta is not None else fb.get("theta")
            vega  = vega  if vega  is not None else fb.get("vega")

        # 2) fallback to last_good cache if still missing core fields
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            lg = prev_last_good.get(m["contract"])
            if isinstance(lg, dict):
                bid = lg.get("bid")     if bid is None else bid
                ask = lg.get("ask")     if ask is None else ask
                oi  = lg.get("oi")      if oi  is None else oi
                vol = lg.get("volume")  if vol is None else vol
                ts  = lg.get("quote_time_utc") or ts

        # update last_good if we now have all core fields
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
            "delta": delta, "gamma": gamma, "theta": theta, "vega": vega,
            "oi": int(oi) if oi is not None else None,
            "volume": int(vol) if vol is not None else None,
            "quote_time_utc": ts
        })

    # ATR cache updates from workers
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
        ema_out[sym] = {"ema30": ema_val, "last_date": dt.datetime.now(UTC).date().isoformat()}

    # Coverage stats + logs
    with_delta = sum(1 for q in quotes if q.get("delta") is not None)
    with_core  = sum(1 for q in quotes if all(q.get(k) is not None for k in ("bid","ask","oi","volume")))
    total_q    = len(quotes)
    greeks_ratio = round(with_delta/total_q, 3) if total_q else None
    core_ratio   = round(with_core/total_q, 3) if total_q else None
    print(f"[relay] quotes_out={total_q} greeks_with_delta={with_delta} core={with_core}")

    # Publish market.json
    market_payload = {
        "meta": {
            "source": "tradier",
            "updated_utc": now_utc_iso(),
            "interval_sec": INTERVAL_SEC,
            "greeks_coverage": {"with_delta": with_delta, "total": total_q, "ratio": greeks_ratio},
            "quotes_core_coverage": {"with_core": with_core, "total": total_q, "ratio": core_ratio}
        },
        "underliers": underliers,
        "quotes": quotes,
        "chain_metrics": chain_metrics,
        "state": {
            "ema": ema_out,
            "last_good_quotes": last_good,
            "atr_cache": prev_atr,
            "wing_selection": wing_selection
        }
    }
    gh_put(GIST_ID_MKT, "market.json", market_payload)

if __name__ == "__main__":
    main()
