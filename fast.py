#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
High Delivery Quantity Scanner (Free, NSE)
-----------------------------------------
- Universe: fast official sec_list.csv (cached) -> fallback EQUITY_L.csv
- Bhav prefilter: legacy cmDDMMMYYYYbhav.csv.zip (skip illiquid/no-move)
- Delivery data: NSE "priceVolumeDeliverable" (per symbol, short timeouts, retries, rate limiting)
- Optional async mode (httpx) with concurrency
- Filters:
    %chg > 0,
    DeliverableQty >= 10,000,
    Delivery spike >= 4x (vs last N days avg),
    Mcap >= ₹100 cr,
    **NEW:** Last close > EMA(N) (default N=200) — can disable with --no-ema-filter
- Output: Excel (Candidates + Raw), TradingView links, optional Telegram alert

Usage examples:
  python screener.py
  python screener.py --use-async --concurrency 10
  python screener.py --symbols RELIANCE,SBIN,TATAMOTORS
  python screener.py --min-deliv-qty 20000 --spike-multiple 5
  python screener.py --universe file --universe-file my_watchlist.csv
  python screener.py --ema-days 100
  python screener.py --no-ema-filter
"""

import os
import re
import time
import zipfile
import argparse
import math
import random
import datetime as dt
from io import StringIO, BytesIO
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

import requests
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
load_dotenv()

# Optional goodies
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# Optional async turbo
try:
    import httpx, asyncio, random as _random  # keep random alias separate for async jitter
    HAS_HTTPX = True
except Exception:
    HAS_HTTPX = False

# ---------- CONFIG ----------
DEFAULT_SPIKE_MULTIPLE = 3.0
DEFAULT_AVG_DAYS = 5
DEFAULT_MIN_DELIV_QTY = 10_000
DEFAULT_MARKET_CAP_MIN_INR = 1_000_000_000  # ₹100 cr
DEFAULT_EMA_DAYS = 200
OUTPUT_DIR = "output"
CACHE_DIR = "cache"

# Requests settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
GET_TIMEOUT = (10, 10)  # (connect, read) seconds

# NSE endpoints (official pages list these datasets; sec_list.csv is linked on "Securities available for Trading")
NSE_BASE = "https://www.nseindia.com"
NSE_API = NSE_BASE + "/api/historicalOR/generateSecurityWiseHistoricalData"
SEC_LIST = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"
EQUITY_L = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# ---------- UTILS ----------
def ist_today() -> dt.date:
    return dt.datetime.now(IST).date()

def fmt_date(d: dt.date) -> str:
    return d.strftime("%d-%m-%Y")

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

class RateLimiter:
    """Steady-drip limiter so we avoid server throttling spikes."""
    def __init__(self, rate_per_sec: float):
        self.min_interval = 1.0 / max(rate_per_sec, 0.1)
        self.next_ts = time.perf_counter()

    def wait(self):
        now = time.perf_counter()
        if now < self.next_ts:
            time.sleep(self.next_ts - now)
        self.next_ts = max(now, self.next_ts) + self.min_interval + (0.0 if self.min_interval > 0.06 else random.uniform(0, 0.02))

def build_tradingview_link(symbol: str) -> str:
    return f"https://in.tradingview.com/chart/?symbol=NSE:{symbol}"

# ---------- ROBUST SESSION (fail-fast + retries + pooling) ----------
def init_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4, connect=3, read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": NSE_BASE + "/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    })
    try:
        s.get(NSE_BASE, timeout=GET_TIMEOUT)  # warm cookies
    except requests.RequestException:
        pass
    return s

# ---------- UNIVERSE (fast, cached) ----------
def load_csv_with_cache(session: requests.Session, url: str, cache_name: str, max_age_hours=24) -> pd.DataFrame:
    ensure_dirs()
    path = os.path.join(CACHE_DIR, cache_name)
    if os.path.exists(path):
        age = (time.time() - os.path.getmtime(path)) / 3600.0
        if age <= max_age_hours:
            try:
                return pd.read_csv(path)
            except Exception:
                pass
    r = session.get(url, timeout=GET_TIMEOUT)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return pd.read_csv(path)

def read_universe(series_filter=("EQ",), cache_hours=24) -> pd.DataFrame:
    s = init_session()
    # Try fast official sec_list.csv first
    try:
        df = load_csv_with_cache(s, SEC_LIST, "sec_list.csv", cache_hours)
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        out = df[df["SERIES"].isin(series_filter)].copy()
        out["SYMBOL"] = out["SYMBOL"].astype(str).str.strip().str.upper()
        if not out.empty:
            return out[["SYMBOL", "SERIES"]].drop_duplicates()
    except Exception:
        pass
    # Fallback to EQUITY_L.csv
    df = load_csv_with_cache(s, EQUITY_L, "EQUITY_L.csv", cache_hours)
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    out = df[df["SERIES"].isin(series_filter)].copy()
    out["SYMBOL"] = out["SYMBOL"].astype(str).str.strip().str.upper()
    return out[["SYMBOL", "SERIES"]].drop_duplicates()

# ---------- BHAVCOPY PREFILTER ----------
def try_download_legacy_bhav(date: dt.date) -> Optional[pd.DataFrame]:
    mon = date.strftime("%b").upper()
    dd = date.strftime("%d")
    yyyy = date.strftime("%Y")
    url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mon}/cm{dd}{mon}{yyyy}bhav.csv.zip"
    try:
        r = requests.get(url, timeout=(10, 10))
        if r.status_code == 200 and r.content:
            with zipfile.ZipFile(BytesIO(r.content)) as zf:
                name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
                with zf.open(name) as f:
                    bhav = pd.read_csv(f)
            bhav.columns = [c.strip().upper() for c in bhav.columns]
            return bhav
    except Exception:
        return None
    return None

def bhav_prefilter_symbols(min_ttq=10_000, require_pos_change=True) -> Optional[List[str]]:
    # Try today, else go back up to 3 days (weekend/holiday)
    d = ist_today()
    for back in range(0, 4):
        bhav = try_download_legacy_bhav(d - dt.timedelta(days=back))
        if isinstance(bhav, pd.DataFrame) and not bhav.empty:
            # Bhav has PREVCLOSE, CLOSE, TOTTRDQTY, SERIES, SYMBOL
            bhav["PCT_CHANGE"] = (bhav["CLOSE"] - bhav["PREVCLOSE"]) / bhav["PREVCLOSE"] * 100
            cond = (bhav["TOTTRDQTY"] >= min_ttq) & (bhav["SERIES"] == "EQ")
            if require_pos_change:
                cond &= (bhav["PCT_CHANGE"] > 0)
            syms = bhav.loc[cond, "SYMBOL"].astype(str).str.upper().unique().tolist()
            return syms if syms else None
    # If UDiFF wiring is added, hook it here (NSE All Reports lists UDiFF Common Bhavcopy).
    return None

# ---------- DELIVERY CSV PARSING ----------
NA_TOKENS = ["-", "–", "—", "−", "NA", "N/A", ""]

def fetch_pv_deliv_csv(session: requests.Session, symbol: str, start: dt.date, end: dt.date) -> Optional[pd.DataFrame]:
    params = {
        "from": fmt_date(start),
        "to": fmt_date(end),
        "symbol": symbol.replace("&", "%26"),
        "type": "priceVolumeDeliverable",
        "series": "ALL",
        "csv": "true"
    }
    try:
        r = session.get(NSE_API, params=params, timeout=GET_TIMEOUT)
        if r.status_code == 200 and r.text.strip():
            txt = r.text.replace("\x82", "").replace("â¹", "Rs")
            df = pd.read_csv(
                StringIO(txt),
                na_values=NA_TOKENS, keep_default_na=True, skipinitialspace=True, low_memory=False
            )
            df.columns = [c.replace(" ", "") for c in df.columns]
            return df
    except requests.RequestException:
        return None
    return None

def prep_numeric(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    num_cols = [
        "PrevClose","OpenPrice","HighPrice","LowPrice","LastPrice",
        "ClosePrice","AveragePrice","TotalTradedQuantity","TurnoverInRs",
        "No.ofTrades","DeliverableQty"
    ]
    dash_only = re.compile(r"^\s*[–—\-]\s*$")
    for col in num_cols:
        if col in df.columns:
            s = (df[col].astype(str).str.strip()
                 .str.replace(",", "", regex=False)
                 .str.replace("\u2212", "-", regex=False))
            s = s.where(~s.str.match(dash_only), "")  # blank lone dashes
            df[col] = pd.to_numeric(s, errors="coerce")
    if "%DlyQttoTradedQty" in df.columns:
        s = (df["%DlyQttoTradedQty"].astype(str).str.strip()
             .str.replace(",", "", regex=False)
             .str.replace("\u2212", "-", regex=False))
        s = s.where(~s.str.match(dash_only), "")
        df["%DlyQttoTradedQty"] = pd.to_numeric(s, errors="coerce")
    return df.sort_values("Date")

def compute_today_metrics(df: pd.DataFrame, avg_days: int) -> Optional[Dict]:
    if df is None or df.empty:
        return None
    df = prep_numeric(df).dropna(subset=["Date"])
    if df.empty:
        return None
    today_row = df.iloc[-1]
    hist = df.iloc[:-1].tail(avg_days)
    avg_deliv = hist["DeliverableQty"].mean(skipna=True) if not hist.empty else float("nan")
    prev_close = today_row.get("PrevClose", float("nan"))
    close = today_row.get("ClosePrice", float("nan"))
    pct_change = (close - prev_close) / prev_close * 100 if prev_close and not math.isnan(prev_close) else float("nan")
    return {
        "date": today_row["Date"].date() if isinstance(today_row["Date"], pd.Timestamp) else today_row["Date"],
        "prev_close": prev_close,
        "close": close,
        "pct_change": pct_change,
        "total_traded_qty": today_row.get("TotalTradedQuantity", float("nan")),
        "deliverable_qty": today_row.get("DeliverableQty", float("nan")),
        "deliverable_pct": today_row.get("%DlyQttoTradedQty", float("nan")),
        "avg_deliv_qty_last_n": avg_deliv,
        "delivery_spike_multiple": (today_row.get("DeliverableQty", float("nan")) / avg_deliv
                                    if avg_deliv and not math.isnan(avg_deliv) and avg_deliv > 0 else float("nan"))
    }

# ---------- MARKET CAP ----------
def get_market_cap_inr(yahoo_symbol: str) -> Optional[float]:
    try:
        t = yf.Ticker(yahoo_symbol)
        cap = None
        # fast path
        if getattr(t, "fast_info", None) is not None:
            cap = getattr(t.fast_info, "market_cap", None)
        if not cap:
            info = t.get_info()
            cap = info.get("marketCap")
        if isinstance(cap, (int, float)) and cap > 0:
            return float(cap)
    except Exception:
        return None
    return None

# ---------- EMA FILTER (using yfinance daily bars) ----------
def yf_last_close_and_ema(yahoo_symbol: str, ema_days: int = DEFAULT_EMA_DAYS) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (last_close, ema_n). Uses up to ~max(ema_days+40, 260) trading days for warmup.
    """
    try:
        min_days = max(ema_days + 40, 260)
        t = yf.Ticker(yahoo_symbol)
        hist = t.history(period=f"{min_days}d", interval="1d", auto_adjust=False)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None, None
        close = hist["Close"].dropna()
        if close.empty:
            return None, None
        ema = close.ewm(span=ema_days, adjust=False).mean()
        return float(close.iloc[-1]), float(ema.iloc[-1])
    except Exception:
        return None, None

# ---------- EXCEL + TELEGRAM ----------
def export_excel(df_filtered: pd.DataFrame, df_raw: pd.DataFrame, cfg, ema_days: int, require_ema: bool) -> str:
    ensure_dirs()
    path = os.path.join(OUTPUT_DIR, f"HighDelivery_{ist_today().strftime('%Y-%m-%d')}.xlsx")
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        if not df_filtered.empty:
            df_filtered.to_excel(xl, index=False, sheet_name="Candidates")
        if not df_raw.empty:
            df_raw.to_excel(xl, index=False, sheet_name="Raw_All")
        pd.DataFrame([{
            "spike_multiple_threshold": cfg.spike_multiple,
            "avg_days": cfg.avg_days,
            "min_deliverable_qty": cfg.min_deliv_qty,
            "min_market_cap_inr": cfg.min_mcap_inr,
            "ema_days": ema_days,
            "ema_filter_enabled": require_ema,
        }]).to_excel(xl, index=False, sheet_name="Run_Info")
    return path

# --- Telegram helpers (robust) ---
TG_API_BASE = "https://api.telegram.org"

def tg_post(path: str, token: str, payload: dict, files=None) -> dict:
    url = f"{TG_API_BASE}/bot{token}/{path}"
    try:
        if files:
            r = requests.post(url, data=payload, files=files, timeout=(10, 15))
        else:
            r = requests.post(url, json=payload, timeout=(10, 15))
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram error: {data}")
        return data
    except Exception as e:
        print(f"[Telegram] request failed: {e}")
        return {"ok": False, "error": str(e)}

def tg_chunk_and_send(token: str, chat_id: str, text: str, chunk_size: int = 3900):
    # Telegram max text ~4096 chars; keep safety margin
    if not text:
        return
    parts = []
    while len(text) > chunk_size:
        # try to split at a newline near the limit
        cut = text.rfind("\n", 0, chunk_size)
        if cut == -1:
            cut = chunk_size
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    parts.append(text)
    for i, part in enumerate(parts, 1):
        suffix = f"\n\n({i}/{len(parts)})" if len(parts) > 1 else ""
        tg_post("sendMessage", token, {
            "chat_id": chat_id,
            "text": part + suffix,
            "disable_web_page_preview": True
        })

def tg_send_document(token: str, chat_id: str, file_path: str, caption: str = ""):
    try:
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            payload = {"chat_id": chat_id, "caption": caption}
            tg_post("sendDocument", token, payload, files=files)
    except FileNotFoundError:
        print(f"[Telegram] file not found: {file_path}")

def telegram_alert(df_filtered: pd.DataFrame, df_raw: pd.DataFrame, excel_path: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("[Telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; skipping.")
        return

    # Always send a run summary, even if no matches.
    lines = [
        "🚨 High Delivery Scan — run completed",
        f"Date: {ist_today().strftime('%d-%b-%Y')}",
        f"Universe scanned: {len(df_raw) if not df_raw.empty else 0} symbols",
    ]

    if df_filtered is None or df_filtered.empty:
        lines.append("Matches: 0 (no symbols met thresholds).")
        summary = "\n".join(lines)
        tg_chunk_and_send(token, chat_id, summary)
        # Optionally still upload the Excel for transparency
        if excel_path and os.path.exists(excel_path):
            tg_send_document(token, chat_id, excel_path, caption="Excel (no matches)")
        return

    # There are matches — include a concise list (cap to avoid message bloat)
    lines.append(f"Matches: {len(df_filtered)}")
    lines.append("")
    top = df_filtered.head(30)  # keep the message short; rest is in Excel
    for _, r in top.iterrows():
        mcap = r.get("MARKET_CAP_INR", float("nan")) or float("nan")
        mcap_cr = (mcap / 1e7) if math.isfinite(mcap) else float("nan")
        lines.append(
            f"• {r['SYMBOL']}: Δ{r['PCT_CHANGE']:.2f}% | "
            f"Deliv {int(r['DELIVERABLE_QTY']):,} | "
            f"Spike× {r['DELIV_SPIKE_MULT']:.2f} | "
            f"Mcap ₹{mcap_cr:.2f}Cr | {r['TRADINGVIEW']}"
        )

    summary = "\n".join(lines)
    tg_chunk_and_send(token, chat_id, summary)

    # Attach the Excel file
    if excel_path and os.path.exists(excel_path):
        tg_send_document(token, chat_id, excel_path, caption="Full results Excel")

# ---------- ASYNC (optional turbo) ----------
async def async_init_client() -> httpx.AsyncClient:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    limits = httpx.Limits(max_connections=12, max_keepalive_connections=6)
    timeout = httpx.Timeout(connect=15.0, read=15.0, write=15.0, pool=15.0)
    client = httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits, http2=True)
    try:
        await client.get("https://www.nseindia.com/")
    except Exception:
        pass
    return client

async def async_fetch_one(client: httpx.AsyncClient, symbol: str, start: dt.date, end: dt.date) -> Tuple[str, Optional[pd.DataFrame]]:
    params = {
        "from": fmt_date(start), "to": fmt_date(end), "symbol": symbol.replace("&", "%26"),
        "type": "priceVolumeDeliverable", "series": "ALL", "csv": "true"
    }
    for attempt in range(3):
        try:
            r = await client.get(NSE_API, params=params)
            if r.status_code == 200 and r.text.strip():
                txt = r.text.replace("\x82", "").replace("â¹", "Rs")
                df = pd.read_csv(StringIO(txt), na_values=NA_TOKENS,
                                 keep_default_na=True, skipinitialspace=True, low_memory=False)
                df.columns = [c.replace(" ", "") for c in df.columns]
                return symbol, df
        except Exception:
            await asyncio.sleep(0.4 * (attempt + 1))
    return symbol, None

async def async_fetch_many(symbols: List[str], avg_days: int, concurrency: int) -> List[Dict]:
    today = ist_today()
    start = today - dt.timedelta(days=avg_days + 1)
    sem = asyncio.Semaphore(concurrency)
    results: List[Dict] = []
    client = await async_init_client()

    async def worker(sym: str):
        async with sem:
            await asyncio.sleep(_random.uniform(0.02, 0.12))
            _, df = await async_fetch_one(client, sym, start, today)
            m = compute_today_metrics(df, avg_days) if df is not None else None
            if m:
                results.append({
                    "SYMBOL": sym, "DATE": m["date"], "PREV_CLOSE": m["prev_close"],
                    "CLOSE": m["close"], "PCT_CHANGE": m["pct_change"],
                    "TOTAL_TRADED_QTY": m["total_traded_qty"], "DELIVERABLE_QTY": m["deliverable_qty"],
                    "DELIVERABLE_PCT": m["deliverable_pct"], f"AVG_DELIV_LAST_{avg_days}D": m["avg_deliv_qty_last_n"],
                    "DELIV_SPIKE_MULT": m["delivery_spike_multiple"], "TRADINGVIEW": build_tradingview_link(sym),
                })

    try:
        tasks = [asyncio.create_task(worker(s)) for s in symbols]
        if tqdm is not None:
            from tqdm.asyncio import tqdm as async_tqdm
            for _ in await async_tqdm.gather(*tasks, desc=f"Fetching deliveries(concurrency={concurrency})", dynamic_ncols=True):
                pass
        else:
            await asyncio.gather(*tasks)
    finally:
        await client.aclose()
    return results

# ---------- SCAN CFG ----------
@dataclass
class ScanConfig:
    spike_multiple: float = DEFAULT_SPIKE_MULTIPLE
    avg_days: int = DEFAULT_AVG_DAYS
    min_deliv_qty: int = DEFAULT_MIN_DELIV_QTY
    min_mcap_inr: float = DEFAULT_MARKET_CAP_MIN_INR

# ---------- MAIN ----------
def main():
    parser = argparse.ArgumentParser(description="High Delivery Quantity Scanner (Free, NSE)")
    parser.add_argument("--spike-multiple", type=float, default=DEFAULT_SPIKE_MULTIPLE)
    parser.add_argument("--avg-days", type=int, default=DEFAULT_AVG_DAYS)
    parser.add_argument("--min-deliv-qty", type=int, default=DEFAULT_MIN_DELIV_QTY)
    parser.add_argument("--min-mcap-inr", type=float, default=DEFAULT_MARKET_CAP_MIN_INR)
    parser.add_argument("--ema-days", type=int, default=DEFAULT_EMA_DAYS, help="EMA length for price-above-EMA filter")
    parser.add_argument("--no-ema-filter", action="store_true", help="Disable the price>EMA filter")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated NSE symbols to scan (overrides universe)")
    parser.add_argument("--universe", type=str, default="nse", help="nse (default) | file")
    parser.add_argument("--universe-file", type=str, default="", help="CSV path with SYMBOL column if --universe file")
    parser.add_argument("--universe-cache-hours", type=int, default=24)
    parser.add_argument("--no-bhav-prefilter", action="store_true", help="Disable bhav prefilter step")
    parser.add_argument("--use-async", action="store_true", help="Use httpx async mode if available")
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("NSE_CONCURRENCY", "8")))
    args = parser.parse_args()

    cfg = ScanConfig(args.spike_multiple, args.avg_days, args.min_deliv_qty, args.min_mcap_inr)
    ema_days = args.ema_days
    require_ema = (not args.no_ema_filter)

    # Universe
    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        print(f"Symbols to scan: {len(symbols)} (manual list)")
    else:
        if args.universe == "file":
            if not args.universe_file or not os.path.exists(args.universe_file):
                raise FileNotFoundError("--universe-file must point to a CSV with a SYMBOL column")
            dfu = pd.read_csv(args.universe_file)
            col = "SYMBOL" if "SYMBOL" in dfu.columns else ("Symbol" if "Symbol" in dfu.columns else None)
            if not col:
                raise ValueError("Your CSV must have a SYMBOL or Symbol column")
            symbols = dfu[col].astype(str).str.strip().str.upper().dropna().unique().tolist()
        else:
            print("Loading universe: nse …")
            uni = read_universe()
            symbols = uni["SYMBOL"].tolist()
        print(f"Symbols to scan: {len(symbols)}")

    print(f"Thresholds — Spike×≥{cfg.spike_multiple}, DelivQty≥{cfg.min_deliv_qty:,}, "
          f"Mcap≥₹{cfg.min_mcap_inr:,.0f}, %Chg>0")
    print(f"EMA filter: {'ON' if require_ema else 'OFF'} (price above EMA-{ema_days})")

    # Bhav prefilter
    if not args.no_bhav_prefilter:
        pref = bhav_prefilter_symbols(min_ttq=cfg.min_deliv_qty, require_pos_change=True)
        if pref:
            before = len(symbols)
            symset = set(symbols)
            symbols = [s for s in pref if s in symset]
            print(f"Bhav prefilter: {before} → {len(symbols)} candidates (TTQ≥{cfg.min_deliv_qty:,}, %chg>0).")
        else:
            print("Bhav prefilter not available today; proceeding with full list.")

    # Fetch delivery data
    today = ist_today()
    start = today - dt.timedelta(days=cfg.avg_days + 1)
    raw_records: List[Dict] = []

    if args.use_async and HAS_HTTPX:
        conc = max(2, min(24, args.concurrency))
        print(f"Using async fetch with concurrency={conc} …")
        raw_records = asyncio.run(async_fetch_many(symbols, cfg.avg_days, conc))
    else:
        print("Using requests (steady rate, retries) …")
        session = init_session()
        limiter = RateLimiter(rate_per_sec=5.0)  # steady drip ~5 req/s
        failed: List[str] = []

        iterable = symbols if tqdm is None else tqdm(symbols, desc="Downloading delivery data", unit="stk", dynamic_ncols=True)
        for i, sym in enumerate(iterable, 1):
            limiter.wait()
            df = fetch_pv_deliv_csv(session, sym, start, today)
            if df is None:
                failed.append(sym)
                continue
            m = compute_today_metrics(df, cfg.avg_days)
            if m:
                raw_records.append({
                    "SYMBOL": sym, "DATE": m["date"], "PREV_CLOSE": m["prev_close"], "CLOSE": m["close"],
                    "PCT_CHANGE": m["pct_change"], "TOTAL_TRADED_QTY": m["total_traded_qty"],
                    "DELIVERABLE_QTY": m["deliverable_qty"], "DELIVERABLE_PCT": m["deliverable_pct"],
                    f"AVG_DELIV_LAST_{cfg.avg_days}D": m["avg_deliv_qty_last_n"],
                    "DELIV_SPIKE_MULT": m["delivery_spike_multiple"], "TRADINGVIEW": build_tradingview_link(sym),
                })

        # Second pass gently
        if failed:
            print(f"\nSecond pass on {len(failed)} skipped symbols (gentle pace)…")
            limiter = RateLimiter(rate_per_sec=2.0)
            global GET_TIMEOUT
            GET_TIMEOUT = (10, 20)  # longer read on retry
            iterable2 = failed if tqdm is None else tqdm(failed, desc="Retrying", unit="stk", dynamic_ncols=True)
            for sym in iterable2:
                limiter.wait()
                df = fetch_pv_deliv_csv(session, sym, start, today)
                if df is None:
                    continue
                m = compute_today_metrics(df, cfg.avg_days)
                if m:
                    raw_records.append({
                        "SYMBOL": sym, "DATE": m["date"], "PREV_CLOSE": m["prev_close"], "CLOSE": m["close"],
                        "PCT_CHANGE": m["pct_change"], "TOTAL_TRADED_QTY": m["total_traded_qty"],  # fixed typo
                        "DELIVERABLE_QTY": m["deliverable_qty"], "DELIVERABLE_PCT": m["deliverable_pct"],
                        f"AVG_DELIV_LAST_{cfg.avg_days}D": m["avg_deliv_qty_last_n"],
                        "DELIV_SPIKE_MULT": m["delivery_spike_multiple"], "TRADINGVIEW": build_tradingview_link(sym),
                    })

    df_raw = pd.DataFrame.from_records(raw_records)
    df_filtered = pd.DataFrame()

    if not df_raw.empty:
        # Stage-1 filters
        df_stage = df_raw[
            (df_raw["DELIVERABLE_QTY"] >= cfg.min_deliv_qty) &
            (df_raw["DELIV_SPIKE_MULT"] >= cfg.spike_multiple) &
            (df_raw["PCT_CHANGE"] > 0)
        ].copy()

        # --- NEW: price-above-EMA filter (default ON) ---
        if not df_stage.empty and require_ema:
            ema_vals: List[float] = []
            last_vals: List[float] = []
            keep_mask: List[bool] = []
            ema_iter = df_stage["SYMBOL"].tolist()
            ema_iter = ema_iter if tqdm is None else tqdm(ema_iter, desc=f"EMA{ema_days} filter", unit="stk", dynamic_ncols=True)

            for sym in ema_iter:
                last_close, ema_n = yf_last_close_and_ema(f"{sym}.NS", ema_days=ema_days)
                ema_vals.append(ema_n if ema_n is not None else float("nan"))
                last_vals.append(last_close if last_close is not None else float("nan"))
                keep_mask.append(
                    (last_close is not None) and (ema_n is not None) and (last_close > ema_n)
                )
                time.sleep(0.02)  # be polite to Yahoo endpoints

            df_stage[f"EMA_{ema_days}"] = ema_vals
            df_stage["YF_LAST_CLOSE"] = last_vals
            df_stage = df_stage[pd.Series(keep_mask, index=df_stage.index)].copy()

        # If EMA filter dropped everything, short-circuit
        if df_stage.empty:
            df_filtered = pd.DataFrame()
        else:
            # Market caps for shortlisted only
            mcaps = []
            short_syms = df_stage["SYMBOL"].tolist()
            caps_iter = short_syms if tqdm is None else tqdm(short_syms, desc="Fetching market caps", unit="stk", dynamic_ncols=True)
            for sym in caps_iter:
                mcaps.append(get_market_cap_inr(f"{sym}.NS") or float("nan"))
                time.sleep(0.03)
            df_stage["MARKET_CAP_INR"] = mcaps

            # Final Stage-2: mcap threshold + tidy sort
            df_filtered = df_stage[df_stage["MARKET_CAP_INR"] >= cfg.min_mcap_inr].copy()

        if not df_filtered.empty:
            df_filtered = df_filtered.sort_values(
                ["DELIV_SPIKE_MULT", "DELIVERABLE_QTY"], ascending=False, ignore_index=True
            )

    # Output
    xl_path = export_excel(df_filtered, df_raw, cfg, ema_days=ema_days, require_ema=require_ema)

    print("\n=== Candidates ===")
    if df_filtered.empty:
        print("No matches today for the given thresholds.")
    else:
        for _, r in df_filtered.iterrows():
            mcap_cr = (r["MARKET_CAP_INR"] or 0) / 1e7
            more = ""
            if require_ema and f"EMA_{ema_days}" in r and "YF_LAST_CLOSE" in r:
                try:
                    more = f" | Lc>EMA{ema_days}: {r['YF_LAST_CLOSE']:.2f}>{r[f'EMA_{ema_days}']:.2f}"
                except Exception:
                    more = ""
            print(f"{r['SYMBOL']:>10} | Δ{r['PCT_CHANGE']:.2f}% | Deliv {int(r['DELIVERABLE_QTY']):,} | "
                  f"Spike× {r['DELIV_SPIKE_MULT']:.2f} | Mcap ₹{mcap_cr:.2f}Cr{more} | {r['TRADINGVIEW']}")

    print(f"\nExcel saved to: {xl_path}")

    telegram_alert(df_filtered, df_raw, xl_path)

if __name__ == "__main__":
    main()
