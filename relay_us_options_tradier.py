#!/usr/bin/env python3
# Publishes calendar.json (earnings+macro) and market.json (underliers+selected options)
# every ~5 min via GitHub Actions, using Tradier (US options) + Finnhub (calendars).
# Required repo secrets: TRADIER_TOKEN, FINNHUB_TOKEN, GIST_TOKEN, GIST_ID_CALENDAR, GIST_ID_MARKET

import os, time, json
import datetime as dt
from datetime import timedelta, timezone
import requests

TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN")
TRADIER_BASE  = os.environ.get("TRADIER_BASE", "https://api.tradier.com/v1")
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN")
GIST_TOKEN    = os.environ.get("GIST_TOKEN")
GIST_ID_CAL   = os.environ.get("GIST_ID_CALENDAR")
GIST_ID_MKT   = os.environ.get("GIST_ID_MARKET")

assert TRADIER_TOKEN and FINNHUB_TOKEN and GIST_TOKEN and GIST_ID_CAL and GIST_ID_MKT, "Missing env vars."

HDR_TR = {"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept":"application/json"}
HDR_GH = {"Authorization": f"Bearer {GIST_TOKEN}", "Accept":"application/vnd.github+json"}

IST = dt.timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

TIER_A = ["SPX","SPY","QQQ","TSLA","NVDA","AAPL","MSFT","META","AMD","GOOGL","NFLX","AVGO","IWM","SMH","XBI","TLT","GDX"]

def now_utc_iso(): return dt.datetime.now(UTC).replace(microsecond=0).isoformat()

def to_ist_iso(ts):
    d = dt.datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts,str) else ts
    return d.astimezone(IST).replace(microsecond=0).isoformat()

def http_get_json(url, headers=None, params=None, timeout=20):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status(); return r.json()

def gh_put(gist_id, filename, obj):
    url = f"https://api.github.com/gists/{gist_id}"
    payload = {"files": {filename: {"content": json.dumps(obj, separators=(',',':'))}}}
    r = requests.patch(url, headers=HDR_GH, json=payload, timeout=20)
    r.raise_for_status()

# Finnhub calendars

def fetch_earnings(from_date, to_date):
    j = http_get_json("https://finnhub.io/api/v1/calendar/earnings",
                      params={"from":from_date,"to":to_date,"token":FINNHUB_TOKEN})
    out = []
    for x in (j.get("earningsCalendar") or []):
        sym = (x.get("symbol") or "").upper()
        if sym and x.get("date"):
            when = (x.get("hour") or x.get("time") or "").upper()
            out.append({"symbol":sym,"date":x["date"],"when":when})
    return out


def fetch_macro(from_date, to_date):
    # Finnhub free plans may 403 this endpoint. Fail-open to [] so the relay keeps working.
    try:
        j = http_get_json(
            "https://finnhub.io/api/v1/calendar/economic",
            params={"from": from_date, "to": to_date, "token": FINNHUB_TOKEN}
        )
        arr = (j.get("economicCalendar") or [])
    except Exception:
        arr = []  # no macro events (scanner will treat as no-macro-day)
    out = []
    for x in arr:
        name = (x.get("event") or "").upper()
        if any(k in name for k in ["CPI","PCE","NFP","PAYROLL","FOMC","FED","JOBS"]):
            ts = x.get("time") or x.get("datetime") or (x.get("date")+"T13:30:00Z" if x.get("date") else None)
            if ts:
                out.append({"event": name, "utc_time": dt.datetime.fromisoformat(ts.replace("Z","+00:00")).replace(microsecond=0).isoformat()})
    return out

# Tradier market

def quote_underlier(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/quotes", headers=HDR_TR, params={"symbols":sym})
    q = (j.get("quotes") or {}).get("quote")
    if not q: return None
    if isinstance(q, list): q = q[0]
    last = q.get("last") or q.get("close")
    if not last and q.get("bid") and q.get("ask"): last=(float(q["bid"])+float(q["ask"])) / 2
    return float(last) if last else None


def daily_history(sym, start):
    j = http_get_json(f"{TRADIER_BASE}/markets/history", headers=HDR_TR, params={"symbol":sym,"interval":"daily","start":start})
    days = (j.get("history") or {}).get("day") or []
    return [days] if isinstance(days, dict) else days


def atr_from_history(days, period):
    if len(days) < period+1: return None
    trs=[]; prev=float(days[0]["close"])
    for d in days[1:]:
        h=float(d["high"]); l=float(d["low"]); c=float(d["close"])
        trs.append(max(h-l, abs(h-prev), abs(l-prev))); prev=c
    return sum(trs[-period:])/period if len(trs)>=period else None


def expirations(sym):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/expirations", headers=HDR_TR,
                      params={"symbol":sym,"includeAllRoots":"true","strikes":"false"})
    exps=(j.get("expirations") or {}).get("date") or []
    return [exps] if isinstance(exps,str) else sorted(exps)


def chains(sym, exp):
    j = http_get_json(f"{TRADIER_BASE}/markets/options/chains", headers=HDR_TR, params={"symbol":sym,"expiration":exp})
    opts=(j.get("options") or {}).get("option") or []
    return [opts] if isinstance(opts,dict) else opts


def quotes_options(symbols):
    out=[];
    if not symbols: return out
    for i in range(0,len(symbols),120):
        chunk=",".join(symbols[i:i+120])
        j=http_get_json(f"{TRADIER_BASE}/markets/options/quotes", headers=HDR_TR, params={"symbols":chunk,"greeks":"true"})
        qs=(j.get("quotes") or {}).get("quote") or []
        out.extend([qs] if isinstance(qs,dict) else qs)
    return out


def nearest_friday_on_or_after(date_str):
    y,m,d=map(int,date_str.split("-")); base=dt.date(y,m,d); add=(4-base.weekday())%7
    return (base+timedelta(days=add)).isoformat()


def pick_expiration(sym, desired):
    exps=expirations(sym);
    if not exps: return None
    for e in exps:
        if e>=desired: return e
    return exps[-1]


def pick_targets(chain, spot, how_many=3):
    calls=[c for c in chain if (c.get("option_type") or "").lower()=="call"]
    puts =[p for p in chain if (p.get("option_type") or "").lower()=="put"]
    calls.sort(key=lambda x: abs(float(x.get("strike",0))-spot))
    puts.sort(key=lambda x: abs(float(x.get("strike",0))-spot))
    best=None; bestd=1e12
    for c in calls[:how_many*3]:
        cs=float(c.get("strike",0))
        if not puts: break
        pm=min(puts[:how_many*3], key=lambda p: abs(float(p.get("strike",0))-cs))
        d=abs(cs-spot)+abs(float(pm.get("strike",0))-spot)
        if pm and d<bestd:
            bestd=d; best=(c,pm)
    wings=[]
    for i in range(1,how_many+1):
        if i<len(calls) and i<len(puts): wings.append((calls[i],puts[i]))
    return {"atm":best,"wings":wings}


def spread_pct(bid,ask):
    try:
        bid=float(bid) if bid is not None else None; ask=float(ask) if ask is not None else None
        if not bid or not ask: return None
        mid=(bid+ask)/2;
        return (ask-bid)/mid if mid>0 else None
    except: return None


def main():
    now=dt.datetime.now(UTC)
    from_date=now.date().isoformat()
    to_date=(now+timedelta(days=14)).date().isoformat()

    earn=fetch_earnings(from_date,to_date)
    macro=fetch_macro(from_date,(now+timedelta(days=7)).date().isoformat())

    tierB_syms=sorted({e["symbol"] for e in earn})
    symbols=sorted(set(TIER_A + tierB_syms))

    cal_payload={"meta":{"source":"finnhub","updated_utc":now_utc_iso(),"window_from":from_date,"window_to":to_date},
                 "tierA":TIER_A,
                 "tierB":earn,
                 "macro":[{**m,"ist_time":to_ist_iso(m["utc_time"])} for m in macro]}
    gh_put(GIST_ID_CAL,"calendar.json",cal_payload)

    underliers=[]; opt_symbols=[]; sel_meta=[]
    hist_start=(now-timedelta(days=120)).date().isoformat()

    for u in symbols:
        try:
            spot=quote_underlier(u)
            if not spot: continue
            days=daily_history(u,hist_start)
            atr5=atr_from_history(days[-40:],5) if days else None
            atr14=atr_from_history(days[-80:],14) if days else None
            underliers.append({"u":u,"spot":spot,"atr5":atr5,"atr14":atr14,"updated_utc":now_utc_iso()})

            e_dates=[x["date"] for x in earn if x["symbol"]==u]
            desired=nearest_friday_on_or_after(e_dates[0]) if e_dates else nearest_friday_on_or_after((now+timedelta(days=7)).date().isoformat())
            expiry=pick_expiration(u,desired)
            if not expiry: continue

            chain=chains(u,expiry)
            if not chain: continue
            picks=pick_targets(chain,spot,how_many=3)

            if picks["atm"]:
                c_atm,p_atm=picks["atm"]
                for leg in (c_atm,p_atm):
                    sym=leg.get("symbol")
                    if sym:
                        opt_symbols.append(sym)
                        sel_meta.append({"u":u,"expiry":expiry,"type":("C" if leg["option_type"]=="call" else "P"),
                                         "strike":float(leg["strike"]),"contract":sym})
            for c_leg,p_leg in picks["wings"]:
                for leg in (c_leg,p_leg):
                    sym=leg.get("symbol")
                    if sym:
                        opt_symbols.append(sym)
                        sel_meta.append({"u":u,"expiry":expiry,"type":("C" if leg["option_type"]=="call" else "P"),
                                         "strike":float(leg["strike"]),"contract":sym})
        except Exception:
            continue

    qts=quotes_options(opt_symbols)
    by_sym={q.get("symbol"):q for q in qts} if qts else {}

    quotes=[]
    for m in sel_meta:
        q=by_sym.get(m["contract"],{})
        bid=q.get("bid"); ask=q.get("ask"); last=q.get("last")
        oi=q.get("open_interest"); vol=q.get("volume")
        iv=None
        if q.get("greeks"): iv=q["greeks"].get("mid_iv") or q["greeks"].get("smv_vol")
        ts=q.get("trade_date") or q.get("updated") or now_utc_iso()
        quotes.append({"u":m["u"],"contract":m["contract"],"expiry":m["expiry"],"type":m["type"],"strike":m["strike"],
                       "bid":float(bid) if bid is not None else None, "ask":float(ask) if ask is not None else None,
                       "last":float(last) if last is not None else None, "spread_pct":spread_pct(bid,ask),
                       "iv":float(iv) if iv is not None else None, "oi":int(oi) if oi is not None else None,
                       "volume":int(vol) if vol is not None else None, "quote_time_utc": ts})

    market_payload={"meta":{"source":"tradier","updated_utc":now_utc_iso(),"interval_sec":300},
                    "underliers":underliers, "quotes":quotes}
    gh_put(GIST_ID_MKT,"market.json",market_payload)

if __name__=="__main__":
    main()
