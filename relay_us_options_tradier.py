#!/usr/bin/env python3
# Relay — Tradier options + optional Finnhub earnings
# Publishes:
#   - market.json   (underliers + option quotes + greeks + mid + chain metrics + market_state + EMA + tape + signals + qual)
#   - history.json  (12–28+ comps; Tier-A equities default; Tier-B optional)
#   - calendar.json (optional summary of Tier-A/Tier-B; Tier-C watchlist may be echoed)
#
# Key additions:
#   • Tier-B enrichment (event ≤10 TD): event-week ± neighbor; ATM + Δ≈0.25/0.35/0.45 + 10/20Δ wings
#   • Tier-C sieve (no calendar needed): nearest-weekly 0.20–0.45Δ singles if ask×100 ≤ cap (default $150),
#     plus ATM and cheap wings ($0.05–$0.40), optional neighbor weekly
#   • chain_metrics.market_state = pre|regular|post|closed; per-leg mid stored
#   • signals & qual for direction hints; NaN/Inf → null; prev_high/low robust

import os, json, re, urllib.parse, math
import datetime as dt
from datetime import timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== ENV / KNOBS =====
TRADIER_TOKEN   = os.environ.get("TRADIER_TOKEN")
TRADIER_BASE    = os.environ.get("TRADIER_BASE", "https://api.tradier.com/v1")
FINNHUB_TOKEN   = os.environ.get("FINNHUB_TOKEN", "")   # optional
GIST_TOKEN      = os.environ.get("GIST_TOKEN")
GIST_ID_MARKET  = os.environ.get("GIST_ID_MARKET")
GIST_ID_HISTORY = os.environ.get("GIST_ID_HISTORY")
GIST_ID_CAL     = os.environ.get("GIST_ID_CALENDAR", "")  # optional; empty = skip

# Runtime/perf
INTERVAL_SEC     = int(os.environ.get("INTERVAL_SEC",   "300"))
RELAY_WORKERS    = int(os.environ.get("RELAY_WORKERS",  "8"))
TIERB_LIMIT      = int(os.environ.get("TIERB_LIMIT",    "40"))

# Tape
TAPE_TIERA_ONLY   = os.environ.get("TAPE_TIERA_ONLY","1").lower() in ("1","true","yes")
TAPE_B_TOPN       = int(os.environ.get("TAPE_B_TOPN","16"))
TAPE_INTERVAL     = os.environ.get("TAPE_INTERVAL","5min")
FALLBACK_INTERVAL = os.environ.get("FALLBACK_INTERVAL","1min")
TAPE_FORCE_REFRESH= os.environ.get("TAPE_FORCE_REFRESH","").lower() in ("1","true","yes")
TAPE_PROXY        = {"SPX":"SPY","XSP":"SPY"}

# Tier-B enrichment
TIERB_MINI_ENABLE        = os.environ.get("TIERB_MINI_ENABLE","1").lower() in ("1","true","yes")
TIERB_MINI_TOPN          = int(os.environ.get("TIERB_MINI_TOPN","0"))   # 0 = all in window
TIERB_EVENT_WINDOW_DAYS  = int(os.environ.get("TIERB_EVENT_WINDOW_DAYS","10"))
TIERB_PRIMARY_DELTA_MID  = float(os.environ.get("TIERB_PRIMARY_DELTA_MID","0.35"))
TIERB_LOTTO_DELTA_MID    = float(os.environ.get("TIERB_LOTTO_DELTA_MID","0.18"))
TIERB_MULTI_WEEKLY       = os.environ.get("TIERB_MULTI_WEEKLY","1").lower() in ("1","true","yes")

# Tier-C sieve (affordable high-vol singles)
TIERC_ENABLE             = os.environ.get("TIERC_ENABLE","1").lower() in ("1","true","yes")
TIERC_DELTA_MIN          = float(os.environ.get("TIERC_DELTA_MIN","0.20"))
TIERC_DELTA_MAX          = float(os.environ.get("TIERC_DELTA_MAX","0.45"))
TIERC_MAX_ASK            = float(os.environ.get("TIERC_MAX_ASK","150.0"))   # dollars/contract
TIERC_CHAIN_OI_MIN       = int(os.environ.get("TIERC_CHAIN_OI_MIN","25000"))
TIERC_SPREAD_PCT_MAX     = float(os.environ.get("TIERC_SPREAD_PCT_MAX","0.12"))
TIERC_SPOT_MIN           = float(os.environ.get("TIERC_SPOT_MIN","5.0"))
TIERC_SPOT_MAX           = float(os.environ.get("TIERC_SPOT_MAX","100.0"))
TIERC_MAX_EXPIRIES_PER_U = int(os.environ.get("TIERC_MAX_EXPIRIES_PER_U","2"))  # nearest weekly + neighbor
TIERC_CHEAP_WINGS_MAX    = float(os.environ.get("TIERC_CHEAP_WINGS_MAX","0.40"))
HV_LIST = [s.strip().upper() for s in os.environ.get("HV_LIST","").split(",") if s.strip()]

# History
MAX_EARNINGS_QTRS      = int(os.environ.get("MAX_EARNINGS_QTRS","28"))
DAILY_LOOKBACK_YEARS   = int(os.environ.get("DAILY_LOOKBACK_YEARS","8"))
FORCE_HISTORY_REFRESH  = os.environ.get("FORCE_HISTORY_REFRESH","").lower() in ("1","true","yes")
HISTORY_TIERB          = os.environ.get("HISTORY_TIERB","0").lower() in ("1","true","yes")

# ===== REQUIRED ENVS =====
assert TRADIER_TOKEN and TRADIER_BASE and GIST_TOKEN and GIST_ID_MARKET and GIST_ID_HISTORY, "Missing env vars."

# ===== HEADERS / TZ =====
HDR_TR = {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept":"application/json"}
HDR_GH = {"Authorization": f"Bearer {GIST_TOKEN}",    "Accept":"application/vnd.github+json"}
UTC    = timezone.utc

# ===== UNIVERSE =====
TIER_A = ["SPX","XSP","SPY","QQQ","TSLA","NVDA","AAPL","MSFT","META","AMD","GOOGL","NFLX","AVGO","IWM","SMH","XBI","TLT","GDX"]
TIER_A_EQUITIES = ["TSLA","NVDA","AAPL","MSFT","META","AMD","GOOGL","NFLX","AVGO"]

# ===== RETRY SESSION =====
def _build_session():
    s = requests.Session()
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.6,
                  status_forcelist=[429,500,502,503,504], allowed_methods=frozenset(["GET"]))
    ad = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", ad); s.mount("http://", ad)
    return s
SESSION = _build_session()
CONNECT_TIMEOUT, READ_TIMEOUT = 6, 30

# ===== SANITIZE =====
def _clean_json(x):
    if isinstance(x, dict):  return {k:_clean_json(v) for k,v in x.items()}
    if isinstance(x, list):  return [_clean_json(v) for v in x]
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)): return None
    return x

# ===== HELPERS =====
def now_utc_iso(): return dt.datetime.now(UTC).replace(microsecond=0).isoformat()

def http_get_json(url, headers=None, params=None, timeout=READ_TIMEOUT):
    r = SESSION.get(url, headers=headers, params=params, timeout=(CONNECT_TIMEOUT, timeout))
    r.raise_for_status(); return r.json()

def gh_put(gist_id, filename, obj):
    payload = _clean_json(obj)
    r = SESSION.patch(f"https://api.github.com/gists/{gist_id}", headers=HDR_GH,
                      json={"files": {filename: {"content": json.dumps(payload, indent=2)}}},
                      timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    r.raise_for_status()

def gh_get(gist_id, filename):
    try:
        j = http_get_json(f"https://api.github.com/gists/{gist_id}", headers=HDR_GH)
        f = (j.get("files") or {}).get(filename)
        if not f or "content" not in f: return None
        return json.loads(f["content"])
    except Exception:
        return None

# ===== TRADIER CORE =====
def quote_underlier(sym):
    try:
        j = http_get_json(f"{TRADIER_BASE}/markets/quotes", headers=HDR_TR, params={"symbols": sym})
        q = (j.get("quotes") or {}).get("quote"); 
        if not q: return None
        if isinstance(q, list): q = q[0]
        last = q.get("last") or q.get("close")
        if last is None and q.get("bid") is not None and q.get("ask") is not None:
            try: last = (float(q["bid"]) + float(q["ask"])) / 2
            except: last = None
        return float(last) if last is not None else None
    except Exception:
        return None

def daily_history(sym, start):
    try:
        j = http_get_json(f"{TRADIER_BASE}/markets/history", headers=HDR_TR,
                          params={"symbol": sym, "interval":"daily", "start": start})
        days = (j.get("history") or {}).get("day") or []
        return [days] if isinstance(days, dict) else days
    except Exception:
        return []

def atr_from_history(days, period):
    if len(days)<period+1: return None
    trs, prev = [], float(days[0]["close"])
    for d in days[1:]:
        h,l,c = float(d["high"]), float(d["low"]), float(d["close"])
        trs.append(max(h-l, abs(h-prev), abs(l-prev))); prev=c
    return sum(trs[-period:])/period if len(trs)>=period else None

def expirations(sym):
    try:
        j=http_get_json(f"{TRADIER_BASE}/markets/options/expirations", headers=HDR_TR,
                        params={"symbol":sym,"includeAllRoots":"true","strikes":"false"})
        exps=(j.get("expirations") or {}).get("date") or []
        return [exps] if isinstance(exps, str) else sorted(exps)
    except Exception:
        return []

def chains(sym, exp):
    try:
        j=http_get_json(f"{TRADIER_BASE}/markets/options/chains", headers=HDR_TR,
                        params={"symbol":sym,"expiration":exp,"greeks":"true"})
        opts=(j.get("options") or {}).get("option") or []
        return [opts] if isinstance(opts, dict) else opts
    except Exception:
        return []

# Quotes batching
_OCC_RE         = re.compile(r'^[A-Z.]{1,6}\d{6}[CP]\d{8}$')
_OCC_TYPE_RE    = re.compile(r'\d{6}([CP])\d{8}$')
_OCC_STRIKE_RE  = re.compile(r'(\d{8})$')
_MAX_URL        = 1500
def _is_valid_occ(sym): return bool(sym) and bool(_OCC_RE.match(sym))
def _occ_type(sym): 
    m=_OCC_TYPE_RE.search(sym or ""); 
    return ("call" if m and m.group(1)=="C" else "put") if m else None
def _occ_strike(sym):
    m=_OCC_STRIKE_RE.search(sym or "")
    try: return int(m.group(1))/1000.0 if m else None
    except: return None
def _fetch_quotes_chunk(chunk_syms):
    j=http_get_json(f"{TRADIER_BASE}/markets/options/quotes", headers=HDR_TR,
                    params={"symbols":",".join(chunk_syms), "greeks":"true"})
    rows=(j.get("quotes") or {}).get("quote") or []
    return [rows] if isinstance(rows, dict) else rows
def quotes_options(symbols):
    out=[]; 
    if not symbols: return out
    seen, syms=set(), []
    for s in symbols:
        if s and s not in seen:
            seen.add(s); 
            if _is_valid_occ(s): syms.append(s)
    i=0
    while i<len(syms):
        chunk=[]
        while i<len(syms):
            test=",".join(chunk+[syms[i]])
            q=urllib.parse.urlencode({"symbols":test,"greeks":"true"})
            if len(f"{TRADIER_BASE}/markets/options/quotes?{q}")>_MAX_URL:
                if not chunk: chunk=[syms[i]]; i+=1
                break
            chunk.append(syms[i]); i+=1
        try:
            out.extend(_fetch_quotes_chunk(chunk))
        except requests.RequestException:
            stack=[chunk]
            while stack:
                part=stack.pop()
                if len(part)==1:
                    try: out.extend(_fetch_quotes_chunk(part))
                    except requests.RequestException: pass
                else:
                    mid=len(part)//2
                    stack.append(part[:mid]); stack.append(part[mid:])
    return out

# Timesales
def timesales(sym, interval="5min", day=None):
    if day is None: day=dt.datetime.now(UTC).date().isoformat()
    start=f"{day} 00:00"; end=f"{day} 23:59"
    try:
        j=http_get_json(f"{TRADIER_BASE}/markets/timesales", headers=HDR_TR,
                        params={"symbol":sym,"interval":interval,"start":start,"end":end,"session_filter":"all"},
                        timeout=30)
        rows=(j.get("series",{}) or {}).get("data") or []
        return [rows] if isinstance(rows, dict) else rows
    except Exception:
        return []

def compute_vwap(bars):
    num=den=0.0
    for r in bars:
        v=float(r.get("volume") or 0)
        p=r.get("close"); p=float(p if p is not None else (r.get("price") or 0))
        num+=v*p; den+=v
    return (num/den) if den>0 else None

def rsi(vals, n=5):
    if len(vals)<n+1: return None
    gains=losses=0.0
    for i in range(-n,0):
        ch=vals[i]-vals[i-1]
        gains+=max(ch,0); losses+=max(-ch,0)
    if losses==0: return 100.0
    rs=(gains/n)/(losses/n); return 100-100/(1+rs)

# ===== Market state =====
def market_state_now(now: dt.datetime):
    # Simple UTC heuristic (EDT regular 13:30–20:00). Weekends -> closed.
    wd = now.weekday()  # 0=Mon..6=Sun
    if wd>=5: return "closed"
    hm = now.hour*60 + now.minute
    if 13*60+30 <= hm < 20*60: return "regular"
    return "pre" if hm < 13*60+30 else "post"

# ===== Selection & metrics =====
def nearest_friday_on_or_after(date_str):
    y,m,d = map(int, date_str.split("-")); base=dt.date(y,m,d)
    add=(4-base.weekday())%7; return (base+timedelta(days=add)).isoformat()

def pick_expiration(sym, desired):
    exps=expirations(sym); 
    if not exps: return None
    for e in exps:
        if e>=desired: return e
    return exps[-1]

def nearest_weekly_for_event(sym, earnings_date, window_days=7):
    exps=expirations(sym); 
    if not exps: return None
    e=dt.date.fromisoformat(earnings_date)
    target=nearest_friday_on_or_after(e.isoformat())
    for d in exps:
        if d>=target: return d
    best=None; bd=999
    for d in exps:
        dd=abs((dt.date.fromisoformat(d)-e).days)
        if dd<=window_days and dd<bd: bd, best=dd, d
    return best or exps[-1]

def neighbor_weeklies(sym, event_expiry, window_days=7):
    exps=expirations(sym) or []
    if not exps or not event_expiry: return [event_expiry] if event_expiry else []
    e=dt.date.fromisoformat(event_expiry); prev=nextd=None
    for d in reversed(exps):
        if d<event_expiry and abs((dt.date.fromisoformat(d)-e).days)<=window_days: prev=d; break
    for d in exps:
        if d>event_expiry and abs((dt.date.fromisoformat(d)-e).days)<=window_days: nextd=d; break
    out=[event_expiry]; 
    if prev: out.append(prev)
    if nextd: out.append(nextd)
    return out

def pick_targets(chain_rows, spot, how_many=5):
    calls=[c for c in chain_rows if (c.get("option_type") or "").lower()=="call"]
    puts =[p for p in chain_rows if (p.get("option_type") or "").lower()=="put"]
    calls.sort(key=lambda x: abs(float(x.get("strike",0))-spot))
    puts .sort(key=lambda x: abs(float(x.get("strike",0))-spot))
    best=None; bestd=1e12
    for c in calls[:how_many*3]:
        cs=float(c.get("strike",0))
        if not puts: break
        pm=min(puts[:how_many*3], key=lambda p: abs(float(p.get("strike",0))-cs))
        d=abs(cs-spot)+abs(float(pm.get("strike",0))-cs)
        if pm and d<bestd: bestd,best=d,(c,pm)
    wings=[]
    for i in range(1,how_many+1):
        if i<len(calls) and i<len(puts): wings.append((calls[i],puts[i]))
    return {"atm":best,"wings":wings}

def spread_pct(bid, ask):
    try:
        bid=float(bid) if bid is not None else None
        ask=float(ask) if ask is not None else None
        if not bid or not ask: return None
        mid=(bid+ask)/2.0; return (ask-bid)/mid if mid>0 else None
    except Exception:
        return None

def chain_metrics_from_chain(chain_rows):
    oi_sum, spreads, notional = 0, [], 0.0
    for o in chain_rows:
        try:
            oi=int(o.get("open_interest") or 0); vol=int(o.get("volume") or 0)
            bid=float(o.get("bid")) if o.get("bid") is not None else None
            ask=float(o.get("ask")) if o.get("ask") is not None else None
            oi_sum += oi
            if bid and ask and bid>0 and ask>0:
                mid=(bid+ask)/2.0; spreads.append((ask-bid)/mid)
                if vol>0: notional += vol*mid*100.0
        except Exception:
            continue
    spreads.sort()
    med = spreads[len(spreads)//2] if spreads else None
    return {"chain_oi_total":oi_sum,"chain_median_spread_pct":med,"chain_notional_today":notional}

def ema_update(prev, x, alpha=0.15): return x if prev is None else (alpha*x+(1-alpha)*prev)

# ===== FINNHUB (optional) =====
def fetch_earnings(from_date, to_date):
    if not FINNHUB_TOKEN: return []
    try:
        j=http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                        params={"from":from_date,"to":to_date,"token":FINNHUB_TOKEN})
    except Exception:
        return []
    out=[]; 
    for x in (j.get("earningsCalendar") or []):
        sym=(x.get("symbol") or "").upper()
        if sym and x.get("date"):
            when=(x.get("hour") or x.get("time") or "").upper()
            out.append({"symbol":sym,"date":x["date"],"when":when})
    return out

def fh_earnings_history(symbol, max_quarters=MAX_EARNINGS_QTRS):
    if not FINNHUB_TOKEN: return []
    dates=[]
    try:
        j=http_get_json("https://finnhub.io/api/v1/stock/earnings",
                        params={"symbol":symbol,"token":FINNHUB_TOKEN})
        for r in (j or []):
            d=r.get("date") or r.get("period"); 
            if d: dates.append(str(d)[:10])
    except Exception: dates=[]
    dates=sorted(set(dates), reverse=True)
    if len(dates)<max_quarters:
        # fallback sweep
        today=dt.date.today(); start_year=today.year-8
        for yr in range(start_year, today.year+1):
            try:
                y1=dt.date(yr,1,1).isoformat(); y2=dt.date(yr,12,31).isoformat()
                j=http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                                params={"from":y1,"to":y2,"token":FINNHUB_TOKEN})
                for x in (j.get("earningsCalendar") or []):
                    if (x.get("symbol") or "").upper()==symbol and x.get("date"): dates.append(x["date"])
            except Exception: pass
        dates=sorted(set(dates), reverse=True)
    return dates[:max_quarters]

def compute_comps(symbol, earn_dates):
    try:
        start=(dt.datetime.now(UTC)-timedelta(days=DAILY_LOOKBACK_YEARS*365)).date().isoformat()
        days=daily_history(symbol, start)
        if not days: return []
        idx={str(d["date"]):{"o":float(d["open"]),"h":float(d["high"]),"l":float(d["low"]),"c":float(d["close"])} for d in days}
        out=[]
        for d in earn_dates:
            d0=dt.date.fromisoformat(d)
            d_1=next(((d0-timedelta(days=k)).isoformat() for k in range(1,4) if (d0-timedelta(days=k)).isoformat() in idx), None)
            d1 =next(((d0+timedelta(days=k)).isoformat() for k in range(1,4) if (d0+timedelta(days=k)).isoformat() in idx), None)
            bar_d0=idx.get(d)
            if not bar_d0: continue
            if d_1:
                c_1=idx[d_1]["c"]; o0=bar_d0["o"]; h0=bar_d0["h"]; l0=bar_d0["l"]
                gap=abs(o0-c_1)/c_1; rng=(h0-l0)/c_1
                out.append({"date":d,"when":"BMO","gap_pct":round(gap,6),"range_pct":round(rng,6)})
            elif d1:
                c0=bar_d0["c"]; o1=idx[d1]["o"]; h1=idx[d1]["h"]; l1=idx[d1]["l"]
                gap=abs(o1-c0)/c0; rng=(h1-l1)/c0
                out.append({"date":d,"when":"AMC","gap_pct":round(gap,6),"range_pct":round(rng,6)})
        return out
    except Exception:
        return []

# ===== MAIN =====
def main():
    now = dt.datetime.now(UTC)
    from_date = now.date().isoformat(); to_date = (now + timedelta(days=14)).date().isoformat()
    m_state = market_state_now(now)

    # Tier-B earnings
    earn = fetch_earnings(from_date, to_date) if FINNHUB_TOKEN else []
    if TIERB_LIMIT and TIERB_LIMIT>0 and len(earn)>TIERB_LIMIT:
        earn = sorted(earn, key=lambda x:x["date"])[:TIERB_LIMIT]
    tierB_syms = sorted({e["symbol"] for e in earn})
    symbols    = sorted(set(TIER_A + tierB_syms))
    print(f"[relay] symbols={len(symbols)} tierB={len(tierB_syms)} market_state={m_state}")

    # Tier-B window
    e_by_sym={}
    if TIERB_MINI_ENABLE:
        for x in earn:
            try:
                d=dt.date.fromisoformat(x["date"])
                if 0 <= (d - now.date()).days <= TIERB_EVENT_WINDOW_DAYS:
                    e_by_sym.setdefault(x["symbol"],[]).append(x["date"])
            except Exception: pass

    # Load previous market state
    prev_market = gh_get(GIST_ID_MARKET, "market.json") or {}
    prev_state  = prev_market.get("state", {}) if isinstance(prev_market, dict) else {}
    prev_ema    = prev_state.get("ema", {}) if isinstance(prev_state, dict) else {}
    prev_last   = prev_state.get("last_good_quotes", {}) if isinstance(prev_state, dict) else {}
    prev_atr    = prev_state.get("atr_cache", {}) if isinstance(prev_state, dict) else {}
    prev_tape   = prev_state.get("tape", {}) if isinstance(prev_state, dict) else {}
    prev_signals= prev_state.get("signals", {}) if isinstance(prev_state, dict) else {}
    prev_qual   = prev_state.get("qual", {}) if isinstance(prev_state, dict) else {}

    # Collections
    hist_start=(now - timedelta(days=120)).date().isoformat()
    atr_updates={}; underliers=[]; chain_metrics=[]; sel_meta=[]; opt_symbols=[]
    chain_fb={}

    # Per-symbol worker
    def process_symbol(u):
        try:
            spot=quote_underlier(u)
            if not spot: return None
            # ATR cache
            cache=prev_atr.get(u)
            if cache and cache.get("asof")==now.date().isoformat():
                atr5, atr14 = cache.get("atr5"), cache.get("atr14")
            else:
                days=daily_history(u, hist_start)
                atr5=atr_from_history(days[-40:],5) if days else None
                atr14=atr_from_history(days[-80:],14) if days else None
                if isinstance(atr5,float) and (math.isnan(atr5) or math.isinf(atr5)): atr5=None
                if isinstance(atr14,float) and (math.isnan(atr14) or math.isinf(atr14)): atr14=None
                atr_updates[u]={"asof":now.date().isoformat(),"atr5":atr5,"atr14":atr14}

            # Preferred expiry (Tier-B → event-week; else nearest Friday +7)
            if u in e_by_sym:
                expiry=nearest_weekly_for_event(u, e_by_sym[u][0], window_days=7)
            else:
                expiry=pick_expiration(u, nearest_friday_on_or_after((now + timedelta(days=7)).date().isoformat()))
            under={"u":u,"spot":spot,"atr5":atr5,"atr14":atr14,"updated_utc":now_utc_iso()}
            if not expiry: return {"underlier":under}

            chain_rows=chains(u, expiry)
            if not chain_rows: return {"underlier":under}

            cm=chain_metrics_from_chain(chain_rows); cm["market_state"]=m_state
            # Tier-A gates
            pass_chain=True
            if u in TIER_A:
                if cm.get("chain_oi_total",0)<15000: pass_chain=False
                ms=cm.get("chain_median_spread_pct")
                if ms is not None and ms>0.10: pass_chain=False

            legs=[]
            if pass_chain:
                picks=pick_targets(chain_rows, spot, how_many=5)
                if picks["atm"]: legs+=list(picks["atm"])
                for c_leg,p_leg in picks["wings"]: legs+=[c_leg,p_leg]

            # fallback map for selected legs
            def fnum(x):
                try: return float(x)
                except: return None
            local_fb={}
            leg_syms={l.get("symbol") for l in legs if l.get("symbol")}
            for o in chain_rows:
                if o.get("symbol") in leg_syms:
                    g=o.get("greeks") or {}
                    local_fb[o["symbol"]]={"bid":fnum(o.get("bid")),"ask":fnum(o.get("ask")),
                                           "volume":int(o.get("volume") or 0),"oi":int(o.get("open_interest") or 0),
                                           "iv":fnum(g.get("mid_iv")) or fnum(g.get("smv_vol")),
                                           "delta":fnum(g.get("delta")),"gamma":fnum(g.get("gamma")),
                                           "theta":fnum(g.get("theta")),"vega":fnum(g.get("vega"))}
            return {"underlier":under,"cm":{"u":u,"expiry":expiry,**cm},
                    "sel":[{"u":u,"expiry":expiry,"type":("C" if (l.get("option_type") or _occ_type(l.get("symbol")) )=="call" else "P"),
                            "strike":float(l.get("strike") or _occ_strike(l.get("symbol"))),
                            "contract":l.get("symbol")} for l in legs if l.get("symbol")],
                    "fallback":local_fb}
        except Exception:
            return None

    results=[]
    with ThreadPoolExecutor(max_workers=RELAY_WORKERS) as ex:
        futs={ex.submit(process_symbol,u):u for u in symbols}
        for fut in as_completed(futs):
            r=fut.result()
            if r: results.append(r)

    for r in results:
        if r.get("underlier"): underliers.append(r["underlier"])
        if r.get("cm"): chain_metrics.append(r["cm"])
        if r.get("sel"): sel_meta.extend(r["sel"]); opt_symbols.extend([s["contract"] for s in r["sel"]])
        if r.get("fallback"): chain_fb.update(r["fallback"])

    # ===== Tier-B enrichment: event-week ± neighbor =====
    if TIERB_MINI_ENABLE and e_by_sym:
        # Prune non-optionable
        for s in list(e_by_sym.keys()):
            if not expirations(s): e_by_sym.pop(s,None)
        # Choose enrich set (top-N by OI; else all)
        sorted_b = sorted((cm for cm in chain_metrics if cm["u"] in e_by_sym),
                          key=lambda x:x.get("chain_oi_total",0), reverse=True)
        if TIERB_MINI_TOPN>0:
            enrich = [cm["u"] for cm in sorted_b[:TIERB_MINI_TOPN]]
            if len(enrich)<TIERB_MINI_TOPN:
                for s in e_by_sym.keys():
                    if s not in enrich:
                        enrich.append(s)
                        if len(enrich)>=TIERB_MINI_TOPN: break
            enrich=set(enrich)
        else:
            enrich=set(e_by_sym.keys())

        already=set(m["u"] for m in sel_meta)

        def fnum(x):
            try: return float(x)
            except: return None

        for u in sorted(enrich):
            try:
                if u in already: continue
                spot=quote_underlier(u)
                if not spot: continue
                evt_exp=nearest_weekly_for_event(u, e_by_sym[u][0], window_days=7)
                exp_list=[evt_exp] if evt_exp else []
                if TIERB_MULTI_WEEKLY and evt_exp:
                    exp_list=neighbor_weeklies(u, evt_exp, window_days=7)[:2]
                for expiry in exp_list:
                    rows=chains(u, expiry) or []
                    if not rows: continue
                    calls=sorted([c for c in rows if (c.get("option_type") or "").lower()=="call"],
                                 key=lambda x: abs(float(x.get("strike",0))-spot))
                    puts =sorted([p for p in rows if (p.get("option_type") or "").lower()=="put"],
                                 key=lambda x: abs(float(x.get("strike",0))-spot))
                    legs=[]
                    if calls and puts: legs += [calls[0], puts[0]]  # ATM
                    # Δ singles 0.25/0.35/0.45
                    def best_delta(side,tgt,rank):
                        best=None; bd=1e9
                        pool=[o for o in rows if (o.get("option_type") or "").lower()==side]
                        for o in pool:
                            d=((o.get("greeks") or {}).get("delta"))
                            try: d=abs(float(d)) if d is not None else None
                            except: d=None
                            if d is None: continue
                            dd=abs(d-tgt)
                            if dd<bd: bd,best=dd,o
                        if best: return best
                        pool=sorted(pool, key=lambda x: abs(float(x.get("strike",0))-spot))
                        return pool[min(rank, len(pool)-1)] if pool else None
                    for tgt, rank in ((0.25,3),(TIERB_PRIMARY_DELTA_MID,2),(0.45,1)):
                        c=best_delta("call",tgt,2); p=best_delta("put",tgt,2)
                        if c and c not in legs: legs.append(c)
                        if p and p not in legs: legs.append(p)
                    # wings 10Δ/20Δ
                    for tgt, rank in ((0.10,6),(0.20,4)):
                        c=best_delta("call",tgt,4); p=best_delta("put",tgt,4)
                        if c and c not in legs: legs.append(c)
                        if p and p not in legs: legs.append(p)
                    # Cheap wings ($0.05–$0.40) bonus
                    cheap=[o for o in rows if o.get("ask") is not None and 0.05 <= float(o.get("ask") or 0) <= TIERC_CHEAP_WINGS_MAX]
                    # pick one per side
                    cheap_call=next((o for o in cheap if (o.get("option_type") or "").lower()=="call"), None)
                    cheap_put =next((o for o in cheap if (o.get("option_type") or "").lower()=="put"), None)
                    if cheap_call and cheap_call not in legs: legs.append(cheap_call)
                    if cheap_put  and cheap_put  not in legs: legs.append(cheap_put)

                    # fallback for these legs
                    leg_syms={l.get("symbol") for l in legs if l.get("symbol")}
                    for o in rows:
                        if o.get("symbol") in leg_syms:
                            g=o.get("greeks") or {}
                            chain_fb[o["symbol"]]={"bid":fnum(o.get("bid")),"ask":fnum(o.get("ask")),
                                                   "volume":int(o.get("volume") or 0),"oi":int(o.get("open_interest") or 0),
                                                   "iv":fnum(g.get("mid_iv")) or fnum(g.get("smv_vol")),
                                                   "delta":fnum(g.get("delta")),"gamma":fnum(g.get("gamma")),
                                                   "theta":fnum(g.get("theta")),"vega":fnum(g.get("vega"))}
                    # add to batch
                    for l in legs:
                        sym=l.get("symbol")
                        if not sym: continue
                        sel_meta.append({"u":u,"expiry":expiry,
                                         "type":("C" if (l.get("option_type") or _occ_type(sym))=="call" else "P"),
                                         "strike":float(l.get("strike") or _occ_strike(sym)),
                                         "contract":sym})
                        opt_symbols.append(sym)
            except Exception:
                continue

    # ===== Tier-C sieve (affordable high-vol singles; nearest weekly + neighbor) =====
    if TIERC_ENABLE:
        # Start from HV list + any chain we already computed with decent OI
        hv_seed = set(HV_LIST) | {cm["u"] for cm in chain_metrics if cm.get("chain_oi_total",0)>=TIERC_CHAIN_OI_MIN}
        already={(m["u"], m["expiry"]) for m in sel_meta}
        def fnum(x): 
            try: return float(x)
            except: return None
        for u in sorted(hv_seed):
            try:
                spot=quote_underlier(u)
                if spot is None: continue
                # Filters: spread and spot window via the nearest weekly chain metrics if present
                desired = nearest_friday_on_or_after((now + timedelta(days=7)).date().isoformat())
                exp0 = pick_expiration(u, desired)
                if not exp0: continue
                exps=[exp0]
                # optional neighbor
                exps2 = neighbor_weeklies(u, exp0, window_days=7)
                if TIERC_MAX_EXPIRIES_PER_U>1 and len(exps2)>1:
                    # include exp0 (already), choose one neighbor
                    if exps2[0]==exp0 and len(exps2)>1: exps.append(exps2[1])
                # loop expiries
                for expiry in exps:
                    if (u,expiry) in already: continue
                    rows=chains(u, expiry) or []
                    if not rows: continue
                    cm=chain_metrics_from_chain(rows)
                    cm["market_state"]=m_state
                    # liquidity or spot window
                    med=cm.get("chain_median_spread_pct")
                    liq_ok=(med is not None and med<=TIERC_SPREAD_PCT_MAX)
                    spot_ok=(TIERC_SPOT_MIN <= spot <= TIERC_SPOT_MAX)
                    if not (liq_ok or spot_ok): 
                        continue
                    # pick Δ-band singles within affordability
                    legs=[]
                    def in_band(o):
                        d=((o.get("greeks") or {}).get("delta"))
                        try: d=abs(float(d)) if d is not None else None
                        except: d=None
                        return (d is not None) and (TIERC_DELTA_MIN <= d <= TIERC_DELTA_MAX)
                    def ask_ok(o):
                        a=fnum(o.get("ask"))
                        if a is None:
                            b=fnum(o.get("bid"))
                            a = b+0.02 if b is not None else None
                        return (a is not None) and (a*100.0 <= TIERC_MAX_ASK)
                    picks=[o for o in rows if in_band(o) and ask_ok(o)]
                    # prefer mid-delta first
                    def d_score(o):
                        d=abs(float((o.get("greeks") or {}).get("delta") or 0.0))
                        return abs(d-0.35)
                    picks=sorted(picks, key=d_score)[:6]
                    # add ATM
                    calls=sorted([c for c in rows if (c.get("option_type") or "").lower()=="call"],
                                 key=lambda x: abs(float(x.get("strike",0))-spot))
                    puts =sorted([p for p in rows if (p.get("option_type") or "").lower()=="put"],
                                 key=lambda x: abs(float(x.get("strike",0))-spot))
                    if calls and puts: legs += [calls[0], puts[0]]
                    legs += [o for o in picks if o not in legs]
                    # cheap wings
                    cheap=[o for o in rows if o.get("ask") is not None and 0.05 <= fnum(o.get("ask")) <= TIERC_CHEAP_WINGS_MAX]
                    cheap_call=next((o for o in cheap if (o.get("option_type") or "").lower()=="call"), None)
                    cheap_put =next((o for o in cheap if (o.get("option_type") or "").lower()=="put"),  None)
                    if cheap_call and cheap_call not in legs: legs.append(cheap_call)
                    if cheap_put  and cheap_put  not in legs: legs.append(cheap_put)
                    # fallback for these legs
                    leg_syms={l.get("symbol") for l in legs if l.get("symbol")}
                    for o in rows:
                        if o.get("symbol") in leg_syms:
                            g=o.get("greeks") or {}
                            chain_fb[o["symbol"]]={"bid":fnum(o.get("bid")),"ask":fnum(o.get("ask")),
                                                   "volume":int(o.get("volume") or 0),"oi":int(o.get("open_interest") or 0),
                                                   "iv":fnum(g.get("mid_iv")) or fnum(g.get("smv_vol")),
                                                   "delta":fnum(g.get("delta")),"gamma":fnum(g.get("gamma")),
                                                   "theta":fnum(g.get("theta")),"vega":fnum(g.get("vega"))}
                    for l in legs:
                        sym=l.get("symbol")
                        if not sym: continue
                        sel_meta.append({"u":u,"expiry":expiry,
                                         "type":("C" if (l.get("option_type") or _occ_type(sym))=="call" else "P"),
                                         "strike":float(l.get("strike") or _occ_strike(sym)),
                                         "contract":sym})
                        opt_symbols.append(sym)

    # ===== Tape whitelist for Tier-B (for runtime control) =====
    if TAPE_B_TOPN:
        sorted_b = sorted((cm for cm in chain_metrics if cm["u"] in tierB_syms),
                          key=lambda x:x.get("chain_oi_total",0), reverse=True)
        tierb_tape_whitelist=set(cm["u"] for cm in sorted_b[:TAPE_B_TOPN])
    else:
        tierb_tape_whitelist=set(tierB_syms)

    # ===== Quotes batching =====
    qts=quotes_options(opt_symbols); bysym={q.get("symbol"):q for q in qts} if qts else {}

    # Build quotes (+ mid) with fallbacks; persist greeks
    last_good=dict(prev_last)
    quotes=[]
    for m in sel_meta:
        q=bysym.get(m["contract"],{})
        bid,ask,last=q.get("bid"),q.get("ask"),q.get("last")
        oi,vol=q.get("open_interest"),q.get("volume")
        def f(x):
            try: return float(x)
            except: return None
        delta=gamma=theta=vega=None; iv=None
        if q.get("greeks"):
            g=q["greeks"]; iv=f(g.get("mid_iv")) or f(g.get("smv_vol"))
            delta=f(g.get("delta")); gamma=f(g.get("gamma")); theta=f(g.get("theta")); vega=f(g.get("vega"))
        ts=q.get("trade_date") or q.get("updated") or now_utc_iso()
        fb=chain_fb.get(m["contract"])
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            if fb:
                bid=fb.get("bid") if bid is None else bid; ask=fb.get("ask") if ask is None else ask
                oi =fb.get("oi")  if oi  is None else oi ; vol=fb.get("volume") if vol is None else vol
        if fb:
            iv=iv if iv is not None else fb.get("iv")
            delta=delta if delta is not None else fb.get("delta")
            gamma=gamma if gamma is not None else fb.get("gamma")
            theta=theta if theta is not None else fb.get("theta")
            vega=vega if vega is not None else fb.get("vega")
        if (bid is None) or (ask is None) or (oi is None) or (vol is None):
            lg=last_good.get(m["contract"])
            if isinstance(lg,dict):
                bid=lg.get("bid") if bid is None else bid; ask=lg.get("ask") if ask is None else ask
                oi =lg.get("oi")  if oi  is None else oi ; vol=lg.get("volume") if vol is None else vol
                ts =lg.get("quote_time_utc") or ts
        mid=None
        try:
            if bid is not None and ask is not None:
                mid=(float(bid)+float(ask))/2.0
        except Exception:
            mid=None
        if (bid is not None) and (ask is not None) and (oi is not None) and (vol is not None):
            last_good[m["contract"]]={"bid":float(bid),"ask":float(ask),"oi":int(oi),"volume":int(vol),
                                      "iv":float(iv) if iv is not None else None,"quote_time_utc":ts}
        quotes.append({"u":m["u"],"contract":m["contract"],"expiry":m["expiry"],"type":m["type"],"strike":m["strike"],
                       "bid":float(bid) if bid is not None else None,
                       "ask":float(ask) if ask is not None else None,
                       "mid":float(mid) if mid is not None else None,
                       "last":float(last) if last is not None else None,
                       "spread_pct":spread_pct(bid,ask),
                       "iv":float(iv) if iv is not None else None,
                       "delta":delta,"gamma":gamma,"theta":theta,"vega":vega,
                       "oi":int(oi) if oi is not None else None,
                       "volume":int(vol) if vol is not None else None,
                       "quote_time_utc":ts})

    # ===== EMA =====
    prev_ema = prev_ema if isinstance(prev_ema, dict) else {}
    ema_out={}
    for cm in chain_metrics:
        sym=cm["u"]; x=cm.get("chain_notional_today",0.0) or 0.0
        prev_val=(prev_ema.get(sym) or {}).get("ema30") if isinstance(prev_ema.get(sym), dict) else None
        ema_out[sym]={"ema30":ema_update(prev_val, x, alpha=0.15),"last_date":now.date().isoformat()}

    # ===== Signals & Qual =====
    signals = dict(prev_signals) if isinstance(prev_signals, dict) else {}
    qual    = dict(prev_qual)    if isinstance(prev_qual, dict)    else {}

    # Build chain index (u,expiry)->rows for signals
    chains_map={}
    for cm in chain_metrics:
        key=(cm["u"], cm["expiry"])
        if key not in chains_map:
            try: chains_map[key]=chains(cm["u"], cm["expiry"]) or []
            except Exception: chains_map[key]=[]

    def delta_pick(rows, side, target, fallback_rank=3):
        best=None; bd=1e9
        rs=[o for o in rows if (o.get("option_type") or "").lower()==side]
        for o in rs:
            d=(o.get("greeks") or {}).get("delta")
            try: d=abs(float(d)) if d is not None else None
            except: d=None
            if d is None: continue
            dd=abs(d-target)
            if dd<bd: bd,best=dd,o
        if best: return best
        rs=sorted(rs, key=lambda x: abs(float(x.get("strike",0)) - float(quote_underlier(x.get("root") or "") or 0)))
        return rs[min(fallback_rank, len(rs)-1)] if rs else None

    def iv_of(row):
        if not row: return None
        g=row.get("greeks") or {}
        try: return float(g.get("mid_iv") if g.get("mid_iv") is not None else g.get("smv_vol"))
        except Exception: return None

    def drift_metrics(sym):
        start=(now - timedelta(days=30)).date().isoformat()
        d=daily_history(sym, start)
        if not d or len(d)<11: return None, None, None, None
        closes=[float(x["close"]) for x in d]
        ret5=(closes[-1]-closes[-6])/closes[-6] if closes[-6]!=0 else None
        ret10=(closes[-1]-closes[-11])/closes[-11] if closes[-11]!=0 else None
        s=daily_history("SPY", start)
        if s and len(s)>=11:
            sc=[float(x["close"]) for x in s]
            spy5=(sc[-1]-sc[-6])/sc[-6] if sc[-6]!=0 else None
            spy10=(sc[-1]-sc[-11])/sc[-11] if sc[-11]!=0 else None
        else:
            spy5=spy10=None
        r5=(ret5-spy5) if (ret5 is not None and spy5 is not None) else None
        r10=(ret10-spy10) if (ret10 is not None and spy10 is not None) else None
        return ret5, ret10, r5, r10

    for urow in underliers:
        u=urow["u"]; cm = next((x for x in chain_metrics if x["u"]==u), None)
        if not cm: continue
        rows = chains_map.get((cm["u"], cm["expiry"]), [])
        if not rows: continue
        calls=[o for o in rows if (o.get("option_type") or "").lower()=="call"]
        puts =[o for o in rows if (o.get("option_type") or "").lower()=="put"]
        c25=delta_pick(rows,"call",0.25,3); p25=delta_pick(rows,"put",0.25,3)
        rr25=None; iv25c=iv_of(c25); iv25p=iv_of(p25)
        if iv25c is not None and iv25p is not None: rr25=iv25c-iv25p
        p10=delta_pick(rows,"put",0.10,6); p30=delta_pick(rows,"put",0.30,2)
        skew_slope=None; iv10p=iv_of(p10); iv30p=iv_of(p30)
        if iv10p is not None and iv30p is not None: skew_slope=iv10p-iv30p
        c_vol=sum(int(o.get("volume") or 0) for o in calls)
        p_vol=sum(int(o.get("volume") or 0) for o in puts)
        vol_ratio=(c_vol/p_vol) if p_vol>0 else (float("inf") if c_vol>0 else None)
        c_oi=sum(int(o.get("open_interest") or 0) for o in calls)
        p_oi=sum(int(o.get("open_interest") or 0) for o in puts)
        oi_diff=c_oi - p_oi
        d5,d10,d5r,d10r=drift_metrics(u)

        signals[u]={"risk_reversal_25d":rr25,"skew_slope":skew_slope,
                    "call_put_vol_ratio_today":vol_ratio,"call_minus_put_oi_proxy":oi_diff,
                    "drift_5d":d5,"drift_10d":d10,"drift_5d_vs_spy":d5r,"drift_10d_vs_spy":d10r,
                    "eps_revision_sign_30d":None,"short_interest_pct":None,"days_to_cover":None}

        # Qual hints (+1/0/−1)
        def sgn(x, pos=0.0, neg=0.0):
            if x is None: return 0
            if x>pos: return +1
            if x<neg: return -1
            return 0
        qual[u]={"risk_reversal_sign": sgn(rr25, 0.0, 0.0),
                 "flow_sign": sgn((vol_ratio-1.0) if isinstance(vol_ratio,(int,float)) else None, 0.2, -0.2),
                 "drift_sign": sgn(d5r if d5r is not None else d10r, 0.02, -0.02)}

    # ===== Tape (prev H/L robust; intraday for Tier-A + Tier-B whitelist) =====
    def _prev_hilo_from_daily(sym):
        days=daily_history(sym,(now - timedelta(days=14)).date().isoformat())
        if days and len(days)>=2:
            pd=days[-2]
            try: return float(pd["high"]), float(pd["low"])
            except Exception: pass
        return None, None

    def _hilo_from_timesales(sym, session_day):
        bars=timesales(sym, interval=TAPE_INTERVAL, day=session_day) or \
             timesales(sym, interval=FALLBACK_INTERVAL, day=session_day)
        if not bars: return None, None
        hi=lo=None
        for r in bars:
            h=r.get("high"); l=r.get("low")
            if h is None: h=r.get("price") or r.get("close")
            if l is None: l=r.get("price") or r.get("close")
            try: h=float(h); l=float(l)
            except Exception: continue
            hi=h if hi is None or h>hi else hi
            lo=l if lo is None or l<lo else lo
        return hi,lo

    prev_tape = prev_tape if isinstance(prev_tape, dict) else {}
    tape_state={}; today_iso=now.date().isoformat()

    def last_trading_day(sym):
        days=daily_history(sym,(now - timedelta(days=10)).date().isoformat())
        if not days or len(days)<2: return today_iso
        dlast=str(days[-1]["date"])
        return dlast if dlast!=today_iso else str(days[-2]["date"])

    def _load_bars(sym, session_day):
        bars=timesales(sym, interval=TAPE_INTERVAL, day=session_day)
        if not bars and TAPE_INTERVAL!=FALLBACK_INTERVAL:
            bars=timesales(sym, interval=FALLBACK_INTERVAL, day=session_day)
        return bars

    # Tier-B tape whitelist
    if TAPE_B_TOPN:
        sorted_b = sorted((cm for cm in chain_metrics if cm["u"] in tierB_syms),
                          key=lambda x:x.get("chain_oi_total",0), reverse=True)
        tierb_tape_whitelist=set(cm["u"] for cm in sorted_b[:TAPE_B_TOPN])
    else:
        tierb_tape_whitelist=set(tierB_syms)

    def build_tape_for(u):
        cached=prev_tape.get(u)
        if not TAPE_FORCE_REFRESH and isinstance(cached,dict) and cached.get("asof")==today_iso and \
           (cached.get("vwap") is not None or cached.get("rsi5") is not None):
            return cached
        tape={"asof":today_iso}
        ph,pl=_prev_hilo_from_daily(u)
        if ph is None or pl is None:
            session_prev=last_trading_day(u)
            ph2,pl2=_hilo_from_timesales(u, session_prev)
            if ph2 is not None: ph=ph2
            if pl2 is not None: pl=pl2
        if ph is not None: tape["prev_high"]=ph
        if pl is not None: tape["prev_low"]=pl

        allow_tierb = (not TAPE_TIERA_ONLY) and ((u in tierb_tape_whitelist) or (u in TIER_A))
        if (u in TIER_A) or allow_tierb:
            session=today_iso
            bars=_load_bars(u, session)
            if not bars:
                session=last_trading_day(u); bars=_load_bars(u, session)
            if not bars and u in TAPE_PROXY:
                proxy=TAPE_PROXY[u]; bars=_load_bars(proxy, today_iso) or _load_bars(proxy, last_trading_day(proxy))
                tape["proxy"]=proxy
            if bars:
                closes=[float((r.get("close") if r.get("close") is not None else r.get("price") or 0))
                        for r in bars if (r.get("close") is not None or r.get("price") is not None)]
                tape["session_date"]=session
                if bars and bars[0].get("open") is not None: tape["open"]=float(bars[0]["open"])
                tape["vwap"]=compute_vwap(bars); tape["rsi5"]=rsi(closes,5)
                vols=[int(r.get("volume") or 0) for r in bars]
                if vols:
                    tape["vol5m_now"]=vols[-1]; tape["vol5m_median"]=sorted(vols)[len(vols)//2]
            else:
                tape["session_date"]=session
        return tape

    for urow in underliers:
        u=urow["u"]; t=build_tape_for(u)
        tape_state[u]=t; urow["tape"]=t

    print(f"[relay] tape symbols={len(tape_state)} with_vwap={sum(1 for t in tape_state.values() if t.get('vwap'))} with_rsi={sum(1 for t in tape_state.values() if t.get('rsi5'))}")

    # ===== Coverage stats =====
    with_delta=sum(1 for q in quotes if q.get("delta") is not None)
    with_core =sum(1 for q in quotes if all(q.get(k) is not None for k in ("bid","ask","oi","volume")))
    total_q   =len(quotes)
    print(f"[relay] quotes_out={total_q} greeks_with_delta={with_delta} core={with_core}")

    # ===== Publish =====
    market_payload={"meta":{"source":"tradier","updated_utc":now_utc_iso(),"interval_sec":INTERVAL_SEC,
                            "greeks_coverage":{"with_delta":with_delta,"total":total_q,
                                               "ratio":round(with_delta/total_q,3) if total_q else None},
                            "quotes_core_coverage":{"with_core":with_core,"total":total_q,
                                                    "ratio":round(with_core/total_q,3) if total_q else None}},
                    "underliers":underliers,
                    "quotes":quotes,
                    "chain_metrics":chain_metrics,
                    "state":{"ema":{k:v for k,v in (prev_state.get('ema') or {}).items()},
                             "last_good_quotes":last_good,
                             "atr_cache":(prev_atr or {}) | (atr_updates or {}),
                             "tape":tape_state,
                             "signals":signals,
                             "qual":qual}}
    gh_put(GIST_ID_MARKET, "market.json", market_payload)

    # Optional: echo Tier-C watchlist into calendar (helps scanners discover Tier-C)
    if GIST_ID_CAL:
        # keep existing meta/tierA/tierB
        cal_payload={"meta":{"source":"finnhub" if FINNHUB_TOKEN else "none","updated_utc":now_utc_iso(),
                             "window_from":from_date,"window_to":to_date},
                     "tierA":TIER_A, "tierB":earn,
                     "tierC":{"high_vol_watch": HV_LIST, 
                              "refresh_policy":{"max_names":60,"min_price":3,"max_price":100}}}
        gh_put(GIST_ID_CAL, "calendar.json", cal_payload)

if __name__ == "__main__":
    main()
