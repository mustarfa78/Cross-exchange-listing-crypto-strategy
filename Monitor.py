import websocket
import json
import time
import hmac
import hashlib
import requests
import re
import uuid
import threading
import sys
import queue
import html
import argparse
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Tuple, Set, Dict, List

# ----------------------------
# Console safety (UTF-8)
# ----------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ----------------------------
# CONFIGURATION
# ----------------------------
BINANCE_API_KEY = "rlwMpOx5Z7AMvViN1PJjlFZoFQLmkNU6Lpgx3CJeYMUzZUWRX70QFbERWil1akVn"
BINANCE_SECRET_KEY = "7kYmbK3aflhZ9VEKkYGjNINpyCFQwYiQQItO80vkQ6nSNczghKSflAsjuo3TwKzA"

TELEGRAM_TOKEN = "8298805298:AAFNlSm5TuyQNXfmeZ_mXe5Uhjt7doyc4Lg"
TELEGRAM_CHAT_ID = "5642266820"

# ----------------------------
# SETTINGS / ENDPOINTS
# ----------------------------
# Binance announcement streams + poller
BINANCE_SAPI_TOPIC = "com_announcement_en"
BINANCE_PUBLIC_WS = "wss://stream.binance.com:9443/ws/all_announcement"
BINANCE_POLL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

# Bybit announcements
BYBIT_URL = "https://api.bybit.com/v5/announcements/index"

# XT (Zendesk Help Center) - Tokens and Trading Pairs Listing section
XT_ZENDESK_SECTION_ID = "900000084163"
XT_ZENDESK_ARTICLES_API = f"https://xtsupport.zendesk.com/api/v2/help_center/en-us/sections/{XT_ZENDESK_SECTION_ID}/articles.json"
XT_POLL_INTERVAL_SEC = 6

# KuCoin announcements API (new listings)
KUCOIN_ANN_URL = "https://api.kucoin.com/api/v3/announcements"
KUCOIN_LANG = "en_US"
KUCOIN_ANN_TYPE = "new-listings"
KUCOIN_POLL_INTERVAL_SEC = 10

# Gate.com new listed announcements
GATE_NEWLISTED_URL = "https://www.gate.com/announcements/newlisted"
GATE_POLL_INTERVAL_SEC = 8

# Kraken asset listings RSS
KRAKEN_ASSET_LISTINGS_RSS = "https://blog.kraken.com/category/product/asset-listings/feed"
KRAKEN_POLL_INTERVAL_SEC = 20

# WEEX new listings wiki
WEEX_NEWLISTINGS_URL = "https://www.weex.com/wiki/new-listings"
WEEX_POLL_INTERVAL_SEC = 12

# BingX Zendesk sections (Spot / Innovation / Futures listing)
BINGX_ZENDESK_BASE = "https://bingxservice.zendesk.com"
BINGX_ZENDESK_LOCALE = "en-001"
BINGX_ZENDESK_API_TMPL = f"{BINGX_ZENDESK_BASE}/api/v2/help_center/{BINGX_ZENDESK_LOCALE}/sections/{{sid}}/articles.json"
BINGX_POLL_INTERVAL_SEC = 8
BINGX_SECTIONS = {
    "BINGX_SPOT": "11257060005007",        # Spot Listing
    "BINGX_INNOVATION": "13117025062927",  # Innovation Listing
    "BINGX_FUTURES": "11257015822991",     # Futures Listing (best-guess; will log if not found)
}

# Bitget new listings category page
BITGET_NEWLISTINGS_URL = "https://www.bitget.com/support/categories/11865590960081"
BITGET_POLL_INTERVAL_SEC = 10

# MEXC announcements (new listings)
MEXC_NEWLISTINGS_URL = "https://www.mexc.com/announcements/new-listings"
MEXC_POLL_INTERVAL_SEC = 10

# ----------------------------
# FUTURES GATE (Binance USDⓈ-M OR MEXC Futures)
# ----------------------------
FAPI_EXCHANGEINFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_CACHE_TTL_SEC = 600  # 10 min

# MEXC futures contracts (public)
# NOTE: This is the commonly used public endpoint for MEXC contracts.
# If MEXC changes it, only this constant + parsing function needs adjustment.
MEXC_CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUTURES_CACHE_TTL_SEC = 600  # 10 min

# ----------------------------
# GLOBAL RULES
# ----------------------------
MAX_AGE = timedelta(hours=1)  # <= 1 hour

# ----------------------------
# TELEGRAM SEND QUEUE (IMPORTANT)
# ----------------------------
# items are (text, parse_mode_or_none)
TELE_Q: "queue.Queue[Tuple[str, Optional[str]]]" = queue.Queue(maxsize=5000)

# ----------------------------
# RINGER
# ----------------------------
RINGER_MESSAGE = "IMPORTANT: Recent Match Found, please /stop to stop the reminder"
RINGER_INTERVAL_SEC = 2      # <-- change this to 1, 2, etc.
RINGER_MAX_SECONDS = 120

_ringer_lock = threading.Lock()
_ringer_until_ts = 0.0
_ringer_stop_event = threading.Event()
_ringer_thread: Optional[threading.Thread] = None

# ----------------------------
# DEDUPE (THREAD-SAFE)
# ----------------------------
SEEN_LOCK = threading.Lock()
SEEN_GLOBAL: Set[str] = set()

# ----------------------------
# FUTURES CACHE (THREAD-SAFE)
# ----------------------------
_bin_fut_lock = threading.Lock()
_bin_fut_loaded_at = 0
_bin_base_to_quotes: Dict[str, Set[str]] = {}

_mexc_fut_lock = threading.Lock()
_mexc_fut_loaded_at = 0
_mexc_bases: Set[str] = set()
_mexc_symbols_by_base: Dict[str, Set[str]] = {}

# ----------------------------
# FILTERS
# ----------------------------
IGNORE_WORDS = {
    # Exchange names / noise that can appear in titles
    "XT", "KUCOIN", "KCS", "WEEX", "BINGX", "BITGET", "GATE", "KRAKEN", "MEXC",
    "BINANCE", "BYBIT",

    # Common words
    "WILL", "SUPPORT", "THE", "AND", "FOR", "WITH", "YOUR", "FROM", "THIS",
    "THAT", "OPEN", "CLOSE", "DAILY", "WEEKLY", "MONTHLY", "YEAR", "EARN", "SPOT",
    "MARGIN", "FUTURES", "CRYPTO", "TOKEN", "COIN", "LIST", "DELIST", "MAINTENANCE",
    "NETWORK", "WALLET", "UPGRADE", "UPDATE", "TIME", "DATE", "NOW", "NEW", "ALL",
    "DEPOSIT", "TRADE", "COMPLETE", "ADDED", "ADD", "LAUNCH", "PROJECT", "REVIEW",
    "REMOVAL", "NOTICE", "PLAN", "OFFLINE", "PERPETUAL", "CONTRACT", "SWAP", "OPTIONS",
    "USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "EUR", "GBP", "TRY", "RUB", "AUD", "CAD",
    "MAINNET", "INTEGRATION", "SERVICES", "TRADING", "PAIRS", "ROUNDS", "SHARE", "REWARDS",
    "WORTH", "CAMPAIGN", "COMPETITION", "UNLOCK", "MEGA", "SIMPLE", "LOCKED", "PRODUCTS",
    "VOTE", "USER", "USERS", "RULES", "TERMS", "PRIZE", "POOL", "DISTRIBUTION", "MARKET",
}
TOPIC_CRYPTOS = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "AVAX"}

# ----------------------------
# Helpers
# ----------------------------
def safe_log(message: str):
    try:
        clean_msg = message.encode("ascii", "ignore").decode("ascii")
        print(clean_msg, flush=True)
    except Exception:
        print("[LOG ERROR] Could not print message", flush=True)


def get_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    return session


def send_telegram_msg(text: str, parse_mode: Optional[str] = "HTML"):
    try:
        TELE_Q.put_nowait((text, parse_mode))
    except queue.Full:
        safe_log("[WARN] Telegram queue full, dropping message")


def telegram_sender():
    safe_log("[STATUS] Telegram Sender Started")
    session = get_session()
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    while True:
        text, parse_mode = TELE_Q.get()
        try:
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode

            for attempt in range(6):
                try:
                    r = session.post(url, json=payload, timeout=10)

                    # Hard errors: bad request/unauthorized/forbidden -> don't retry
                    if r.status_code in (400, 401, 403):
                        safe_log(f"[TELEGRAM HTTP {r.status_code}] {r.text}")
                        break

                    # Rate limit
                    if r.status_code == 429:
                        try:
                            j = r.json()
                            wait = int(j.get("parameters", {}).get("retry_after", 5))
                        except Exception:
                            wait = 5
                        time.sleep(min(max(wait, 1), 60))
                        continue

                    # Server errors
                    if r.status_code >= 500:
                        time.sleep(min(2 ** attempt, 30))
                        continue

                    if r.ok:
                        break

                    # Other retryable client-ish errors
                    safe_log(f"[WARN] Telegram send failed (attempt {attempt+1}/6): HTTP {r.status_code}")
                    time.sleep(min(2 ** attempt, 30))

                except requests.RequestException as e:
                    safe_log(f"[WARN] Telegram send exception (attempt {attempt+1}/6): {e}")
                    time.sleep(min(2 ** attempt, 30))
            else:
                safe_log("[ERROR] Telegram send failed after retries")

        except Exception as e:
            safe_log(f"[ERROR] Telegram sender crashed: {e}")
        finally:
            TELE_Q.task_done()


def get_utc3_time(timestamp_ms: Optional[int] = None):
    if timestamp_ms:
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    else:
        dt_utc = datetime.now(timezone.utc)
    return (dt_utc + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")


def _is_too_old_ms(timestamp_ms: int) -> bool:
    try:
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return (datetime.now(timezone.utc) - dt) > MAX_AGE
    except Exception:
        return False


def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def weex_dt_to_ms(dt_str: str) -> int:
    # Example from WEEX page: 2025/12/26 07:40:00
    # Treat as UTC if not specified.
    dt = datetime.strptime(dt_str.strip(), "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def extract_tickers(text: str) -> List[str]:
    """
    No numbers in screening: only A-Z tickers.
    """
    safe_text = re.sub(r"[^A-Za-z\s\(\)]", "", text)
    bracket_matches = re.findall(r"\(([A-Z]{2,15})\)", safe_text)
    loose_matches = re.findall(r"\b[A-Z]{2,10}\b", safe_text)
    all_potential = bracket_matches + loose_matches
    tickers = list(set([t for t in all_potential if t not in IGNORE_WORDS]))
    return tickers


def should_consider(tickers: List[str]) -> bool:
    if not tickers:
        return False
    return any(t not in TOPIC_CRYPTOS for t in tickers)


def _seen_once(key: str) -> bool:
    with SEEN_LOCK:
        if key in SEEN_GLOBAL:
            return True
        SEEN_GLOBAL.add(key)
        return False


# ----------------------------
# RINGER
# ----------------------------
def _ringer_loop():
    while True:
        with _ringer_lock:
            until_ts = _ringer_until_ts

        now = time.time()
        if _ringer_stop_event.is_set() or until_ts <= now:
            break

        send_telegram_msg(RINGER_MESSAGE, parse_mode=None)
        time.sleep(RINGER_INTERVAL_SEC)

    _ringer_stop_event.set()


def trigger_ringer():
    global _ringer_until_ts, _ringer_thread
    with _ringer_lock:
        _ringer_until_ts = time.time() + RINGER_MAX_SECONDS
        _ringer_stop_event.clear()
        if _ringer_thread is None or not _ringer_thread.is_alive():
            _ringer_thread = threading.Thread(target=_ringer_loop, daemon=True)
            _ringer_thread.start()


def stop_ringer():
    global _ringer_until_ts
    with _ringer_lock:
        _ringer_until_ts = 0.0
        _ringer_stop_event.set()


def ringer_status() -> str:
    with _ringer_lock:
        remaining = max(0.0, _ringer_until_ts - time.time())
    if _ringer_stop_event.is_set() or remaining <= 0:
        return "OFF"
    return f"ON ({int(remaining)}s left)"


# ----------------------------
# Binance USDⓈ-M Futures cache
# ----------------------------
def _refresh_binance_futures_cache(session: requests.Session):
    global _bin_fut_loaded_at, _bin_base_to_quotes
    resp = session.get(FAPI_EXCHANGEINFO_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    base_to_quotes: Dict[str, Set[str]] = {}
    for s in data.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base = s.get("baseAsset")
        quote = s.get("quoteAsset")
        if not base or not quote:
            continue
        base_to_quotes.setdefault(base, set()).add(quote)

    _bin_base_to_quotes = base_to_quotes
    _bin_fut_loaded_at = int(time.time())


def binance_usdm_quotes_for(ticker: str, session: requests.Session) -> Set[str]:
    now = int(time.time())
    with _bin_fut_lock:
        if (now - _bin_fut_loaded_at) > BINANCE_FUTURES_CACHE_TTL_SEC or not _bin_base_to_quotes:
            _refresh_binance_futures_cache(session)
        return set(_bin_base_to_quotes.get(ticker, set()))


# ----------------------------
# MEXC futures cache
# ----------------------------
def _parse_mexc_contracts(payload: dict) -> List[dict]:
    """
    MEXC contract endpoint structures can vary.
    Try common shapes:
      - {"success":true,"code":0,"data":[...]}
      - {"code":0,"data":{"contracts":[...]}}
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("contracts", "list", "items", "rows"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    # fallback
    for k in ("result", "contracts"):
        v = payload.get(k)
        if isinstance(v, list):
            return v
    return []


def _refresh_mexc_futures_cache(session: requests.Session):
    global _mexc_fut_loaded_at, _mexc_bases, _mexc_symbols_by_base
    resp = session.get(MEXC_CONTRACT_DETAIL_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    items = _parse_mexc_contracts(data)

    bases: Set[str] = set()
    syms_by_base: Dict[str, Set[str]] = {}

    for it in items:
        if not isinstance(it, dict):
            continue

        # Try to infer symbol/base/quote
        sym = (it.get("symbol") or it.get("contractCode") or it.get("symbolName") or "").strip()
        base = (it.get("baseCoin") or it.get("base") or it.get("baseAsset") or "").strip().upper()
        quote = (it.get("quoteCoin") or it.get("quote") or it.get("quoteAsset") or "").strip().upper()

        if not base and sym:
            # Common MEXC format: BTC_USDT
            m = re.match(r"^([A-Z]{2,15})[_\-]([A-Z]{2,10})$", sym.upper())
            if m:
                base = m.group(1)
                quote = quote or m.group(2)

        # Keep only USDT-ish quoted futures when detectable; otherwise keep anyway
        if quote and quote not in ("USDT", "USD", "USDC"):
            continue

        if base:
            bases.add(base)
            if sym:
                syms_by_base.setdefault(base, set()).add(sym.upper())

    _mexc_bases = bases
    _mexc_symbols_by_base = syms_by_base
    _mexc_fut_loaded_at = int(time.time())


def mexc_symbols_for(ticker: str, session: requests.Session) -> Set[str]:
    now = int(time.time())
    with _mexc_fut_lock:
        if (now - _mexc_fut_loaded_at) > MEXC_FUTURES_CACHE_TTL_SEC or not _mexc_bases:
            _refresh_mexc_futures_cache(session)
        return set(_mexc_symbols_by_base.get(ticker, set()))


# ----------------------------
# GATE: Binance USDⓈ-M OR MEXC futures
# ----------------------------
def passes_futures_gate(ticker: str, session: requests.Session) -> Tuple[bool, bool, bool, Set[str], Set[str]]:
    """
    Returns:
      allowed, binance_ok, mexc_ok, binance_quotes, mexc_symbols
    """
    ticker = ticker.upper().strip()
    bq = set()
    ms = set()

    try:
        bq = binance_usdm_quotes_for(ticker, session)
    except Exception:
        bq = set()

    try:
        ms = mexc_symbols_for(ticker, session)
    except Exception:
        ms = set()

    bin_ok = len(bq) > 0
    mex_ok = len(ms) > 0
    allowed = bin_ok or mex_ok
    return allowed, bin_ok, mex_ok, bq, ms


def format_token_flags(ticker: str, bin_ok: bool, mex_ok: bool) -> str:
    b1 = "Binance USD-M ✅" if bin_ok else "Binance USD-M ❌"
    b2 = "MEXC Futures ✅" if mex_ok else "MEXC Futures ❌"
    return f"<b>{html.escape(ticker)}</b> ({b1}, {b2})"


# ----------------------------
# CENTRAL ALERT HANDLER (ALL SOURCES)
# ----------------------------
def handle_alert(source: str, uniq_id: str, title: str, ts_ms: int, url: str):
    """
    - Dedupe by (source + uniq_id)
    - <= 1 hour old
    - extract tickers (letters only)
    - only consider if contains at least one non-major ticker
    - gate: Binance USD-M OR MEXC futures
    - send message + trigger ringer
    """
    key = f"{source}:{uniq_id}"
    if _seen_once(key):
        return

    if ts_ms and _is_too_old_ms(int(ts_ms)):
        return

    tickers = extract_tickers(title)
    if not should_consider(tickers):
        return

    session = get_session()  # small session for gate calls (caches reduce load)

    passed_parts = []
    for t in tickers:
        if t in TOPIC_CRYPTOS:
            continue
        allowed, bin_ok, mex_ok, _, _ = passes_futures_gate(t, session)
        if allowed:
            passed_parts.append(format_token_flags(t, bin_ok, mex_ok))

    if not passed_parts:
        safe_log(f"[SKIP] {source}: no tickers passed gate. Extracted={tickers}")
        return

    date_str = get_utc3_time(int(ts_ms)) if ts_ms else get_utc3_time()

    safe_title = html.escape(title, quote=False)
    safe_url = html.escape(url, quote=True)

    msg = (
        f"*** {html.escape(source)} ALERT ***\n"
        f"Time: {html.escape(date_str)} (UTC+3)\n"
        f"Token: {', '.join(passed_parts)}\n"
        f"Title: {safe_title}\n"
        f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
    )
    send_telegram_msg(msg, parse_mode="HTML")
    trigger_ringer()
    safe_log(f"[MATCH] {source}: " + ", ".join([re.sub(r"<.*?>", "", p) for p in passed_parts]))


# ----------------------------
# Binance REST poller
# ----------------------------
def monitor_binance_poll():
    safe_log("[STATUS] Binance Poller Started (Layer 3)")
    session = get_session()
    while True:
        try:
            resp = session.get(
                BINANCE_POLL_URL,
                params={"type": 1, "pageNo": 1, "pageSize": 10},
                timeout=20,
            )
            data = resp.json()

            all_articles = []
            if "data" in data and "catalogs" in data["data"]:
                for catalog in data["data"]["catalogs"]:
                    all_articles.extend(catalog.get("articles", []))

            for article in all_articles[:10]:
                code = str(article.get("code", ""))
                title = article.get("title", "") or ""
                ts = int(article.get("releaseDate", 0) or 0)
                url = f"https://www.binance.com/en/support/announcement/{code}" if code else "https://www.binance.com/en/support/announcement"
                handle_alert("BINANCE", code or title, title, ts, url)

            time.sleep(3)
        except Exception:
            time.sleep(5)


# ----------------------------
# Bybit announcements poller
# ----------------------------
def monitor_bybit():
    safe_log("[STATUS] Bybit Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(
                BYBIT_URL,
                params={"locale": "en-US", "limit": 10, "type": "new_crypto"},
                timeout=30,
            )
            data = resp.json()

            if data.get("retCode") == 0:
                for article in (data.get("result") or {}).get("list", []):
                    url = article.get("url") or ""
                    title = article.get("title") or ""
                    ts = int(article.get("dateTimestamp", 0) or 0)
                    if not url or not title:
                        continue
                    handle_alert("BYBIT", url, title, ts, url)

            time.sleep(5)
        except Exception:
            time.sleep(5)


# ----------------------------
# XT (Zendesk) poller
# ----------------------------
def monitor_xt():
    safe_log("[STATUS] XT Poller Started")
    session = get_session()

    while True:
        try:
            resp = session.get(XT_ZENDESK_ARTICLES_API, params={"per_page": 30, "page": 1}, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])

            for a in articles[:20]:
                aid = str(a.get("id", ""))
                title = a.get("title") or ""
                url = a.get("html_url") or a.get("url") or ""
                created_at = a.get("created_at") or a.get("updated_at") or ""
                if not aid or not title or not created_at:
                    continue
                ts = iso_to_ms(created_at)
                handle_alert("XT", aid, title, ts, url or "https://xtsupport.zendesk.com")

            time.sleep(XT_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[XT ERROR] {e}")
            time.sleep(8)


# ----------------------------
# KuCoin poller
# ----------------------------
def monitor_kucoin():
    safe_log("[STATUS] KuCoin Poller Started")
    session = get_session()

    while True:
        try:
            params = {
                "currentPage": 1,
                "pageSize": 20,
                "annType": KUCOIN_ANN_TYPE,
                "lang": KUCOIN_LANG,
            }
            resp = session.get(KUCOIN_ANN_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            items = (data.get("data") or {}).get("items", []) or []

            for it in items[:25]:
                ann_id = str(it.get("annId", ""))
                title = it.get("annTitle") or ""
                url = it.get("annUrl") or ""
                ts = int(it.get("cTime", 0) or 0)
                if not ann_id or not title:
                    continue
                handle_alert("KUCOIN", ann_id, title, ts, url or "https://www.kucoin.com")

            time.sleep(KUCOIN_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[KUCOIN ERROR] {e}")
            time.sleep(10)


# ----------------------------
# Gate.com poller
# ----------------------------
def _gate_fetch_listing_ids(html_text: str) -> List[str]:
    # Extract article IDs like /announcements/article/49001
    ids = re.findall(r'href="/announcements/article/(\d+)"', html_text)
    # keep order, unique
    out = []
    seen = set()
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _gate_fetch_article(session: requests.Session, aid: str) -> Tuple[str, int, str]:
    url = f"https://www.gate.com/announcements/article/{aid}"
    r = session.get(url, timeout=20)
    r.raise_for_status()
    t = r.text

    # Title in <title> or <h1>
    m_title = re.search(r"<h1[^>]*>(.*?)</h1>", t, flags=re.S | re.I)
    if m_title:
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", m_title.group(1))).strip()
    else:
        m2 = re.search(r"<title[^>]*>(.*?)</title>", t, flags=re.S | re.I)
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", (m2.group(1) if m2 else ""))).strip()

    # Timestamp: "2025-12-26 07:40:00 UTC"
    m_ts = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+UTC", t)
    ts_ms = 0
    if m_ts:
        dt = datetime.strptime(m_ts.group(1) + " " + m_ts.group(2), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)

    return title, ts_ms, url


def monitor_gate():
    safe_log("[STATUS] Gate.com Poller Started")
    session = get_session()

    while True:
        try:
            r = session.get(GATE_NEWLISTED_URL, timeout=20)
            r.raise_for_status()
            ids = _gate_fetch_listing_ids(r.text)

            for aid in ids[:20]:
                # dedupe early
                if _seen_once(f"GATE_ID:{aid}"):
                    continue

                try:
                    title, ts_ms, url = _gate_fetch_article(session, aid)
                    if not title:
                        continue
                    handle_alert("GATE", aid, title, ts_ms, url)
                except Exception:
                    continue

            time.sleep(GATE_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[GATE ERROR] {e}")
            time.sleep(10)


# ----------------------------
# WEEX poller
# ----------------------------
def monitor_weex():
    safe_log("[STATUS] WEEX Poller Started")
    session = get_session()

    while True:
        try:
            r = session.get(WEEX_NEWLISTINGS_URL, timeout=20)
            r.raise_for_status()
            text = r.text

            # Pattern tries to capture: link + title + timestamp (YYYY/MM/DD HH:MM:SS)
            pattern = re.compile(
                r'href="([^"]*/wiki/[^"]+)"[^>]*>\s*([^<]{3,200}?)\s*</a>.*?(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})',
                re.S
            )
            matches = pattern.findall(text)

            for href, raw_title, dt_str in matches[:30]:
                title = re.sub(r"\s+", " ", raw_title).strip()
                if not title or not dt_str:
                    continue
                ts = weex_dt_to_ms(dt_str)
                url = href if href.startswith("http") else ("https://www.weex.com" + href)

                uniq = f"{title}|{dt_str}|{url}"
                handle_alert("WEEX", uniq, title, ts, url)

            time.sleep(WEEX_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[WEEX ERROR] {e}")
            time.sleep(10)


# ----------------------------
# BingX Zendesk poller (Spot / Innovation / Futures listing)
# ----------------------------
def _bingx_fetch_section_articles(session: requests.Session, section_id: str) -> List[dict]:
    url = BINGX_ZENDESK_API_TMPL.format(sid=section_id)
    resp = session.get(url, params={"per_page": 30, "page": 1}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("articles", []) or []


def monitor_bingx():
    safe_log("[STATUS] BingX Poller Started")
    session = get_session()

    while True:
        try:
            for label, sid in BINGX_SECTIONS.items():
                try:
                    articles = _bingx_fetch_section_articles(session, sid)
                except Exception as e:
                    # If a section ID is wrong/removed, don't kill the thread
                    safe_log(f"[BINGX WARN] Section {label} ({sid}) fetch failed: {e}")
                    continue

                for a in articles[:25]:
                    aid = str(a.get("id", ""))
                    title = a.get("title") or ""
                    url = a.get("html_url") or a.get("url") or ""
                    created_at = a.get("created_at") or a.get("updated_at") or ""
                    if not aid or not title or not created_at:
                        continue
                    ts = iso_to_ms(created_at)
                    handle_alert("BINGX", f"{label}:{aid}", title, ts, url or BINGX_ZENDESK_BASE)

            time.sleep(BINGX_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[BINGX ERROR] {e}")
            time.sleep(10)


# ----------------------------
# Bitget poller (New listings category page)
# ----------------------------
def _bitget_extract_article_ids(html_text: str) -> List[str]:
    # Grab article links /support/articles/<id>
    ids = re.findall(r'href="(/support/articles/\d+[^"]*)"', html_text)
    out = []
    seen = set()
    for href in ids:
        # Normalize to the base path without query
        href2 = href.split("?")[0]
        if href2 not in seen:
            seen.add(href2)
            out.append(href2)
    return out


def _bitget_fetch_article(session: requests.Session, path: str) -> Tuple[str, int, str]:
    url = "https://www.bitget.com" + path
    r = session.get(url, timeout=20)
    r.raise_for_status()
    t = r.text

    # Title: try h1 or title tag
    m_title = re.search(r"<h1[^>]*>(.*?)</h1>", t, flags=re.S | re.I)
    if m_title:
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", m_title.group(1))).strip()
    else:
        m2 = re.search(r"<title[^>]*>(.*?)</title>", t, flags=re.S | re.I)
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", (m2.group(1) if m2 else ""))).strip()

    # Try to find a datetime string in the article page.
    # Common patterns: 2025-12-26 07:40:00  OR  Dec 19, 2025
    ts_ms = 0
    m_dt1 = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", t)
    if m_dt1:
        dt = datetime.strptime(m_dt1.group(1) + " " + m_dt1.group(2), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)
    else:
        m_dt2 = re.search(r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})", t)
        if m_dt2:
            dt = datetime.strptime(m_dt2.group(1), "%b %d, %Y").replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)

    return title, ts_ms, url


def monitor_bitget():
    safe_log("[STATUS] Bitget Poller Started")
    session = get_session()

    while True:
        try:
            r = session.get(BITGET_NEWLISTINGS_URL, timeout=20)
            r.raise_for_status()
            paths = _bitget_extract_article_ids(r.text)

            for path in paths[:25]:
                # early dedupe
                if _seen_once(f"BITGET_PATH:{path}"):
                    continue
                try:
                    title, ts_ms, url = _bitget_fetch_article(session, path)
                    if not title:
                        continue
                    handle_alert("BITGET", path, title, ts_ms, url)
                except Exception:
                    continue

            time.sleep(BITGET_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[BITGET ERROR] {e}")
            time.sleep(10)


# ----------------------------
# MEXC announcements poller (new listings)
# ----------------------------
def _mexc_extract_announcement_paths(html_text: str) -> List[str]:
    # Try common announcement link patterns
    # e.g. /support/articles/xxxxx or /announcements/xxxxx
    paths = re.findall(r'href="(/announcements/[^"]+)"', html_text)
    paths += re.findall(r'href="(/support/articles/\d+[^"]*)"', html_text)

    out = []
    seen = set()
    for p in paths:
        p2 = p.split("?")[0]
        if p2 not in seen:
            seen.add(p2)
            out.append(p2)
    return out


def _mexc_fetch_article(session: requests.Session, path: str) -> Tuple[str, int, str]:
    if path.startswith("http"):
        url = path
    else:
        url = "https://www.mexc.com" + path

    r = session.get(url, timeout=20)
    r.raise_for_status()
    t = r.text

    # Title
    m_title = re.search(r"<h1[^>]*>(.*?)</h1>", t, flags=re.S | re.I)
    if m_title:
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", m_title.group(1))).strip()
    else:
        m2 = re.search(r"<title[^>]*>(.*?)</title>", t, flags=re.S | re.I)
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", (m2.group(1) if m2 else ""))).strip()

    # Try meta published time (common on many news pages)
    ts_ms = 0
    m_meta = re.search(r'article:published_time"\s+content="([^"]+)"', t)
    if m_meta:
        try:
            ts_ms = iso_to_ms(m_meta.group(1))
        except Exception:
            ts_ms = 0

    # Fallback: look for "YYYY-MM-DD HH:MM:SS" + optional UTC
    if not ts_ms:
        m_dt = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", t)
        if m_dt:
            dt = datetime.strptime(m_dt.group(1) + " " + m_dt.group(2), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)

    return title, ts_ms, url


def monitor_mexc():
    safe_log("[STATUS] MEXC Poller Started")
    session = get_session()

    while True:
        try:
            r = session.get(MEXC_NEWLISTINGS_URL, timeout=20)
            r.raise_for_status()
            paths = _mexc_extract_announcement_paths(r.text)

            for path in paths[:25]:
                if _seen_once(f"MEXC_PATH:{path}"):
                    continue
                try:
                    title, ts_ms, url = _mexc_fetch_article(session, path)
                    if not title:
                        continue
                    handle_alert("MEXC", path, title, ts_ms, url)
                except Exception:
                    continue

            time.sleep(MEXC_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[MEXC ERROR] {e}")
            time.sleep(10)


# ----------------------------
# Kraken RSS poller
# ----------------------------
def monitor_kraken():
    safe_log("[STATUS] Kraken Poller Started")
    session = get_session()

    while True:
        try:
            r = session.get(KRAKEN_ASSET_LISTINGS_RSS, timeout=20)
            r.raise_for_status()

            root = ET.fromstring(r.text)
            channel = root.find("channel")
            if channel is None:
                time.sleep(KRAKEN_POLL_INTERVAL_SEC)
                continue

            items = channel.findall("item")
            for it in items[:25]:
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                guid = (it.findtext("guid") or link or title).strip()
                pub = (it.findtext("pubDate") or "").strip()

                if not title or not guid:
                    continue

                ts_ms = 0
                if pub:
                    try:
                        dt = parsedate_to_datetime(pub)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        ts_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)
                    except Exception:
                        ts_ms = 0

                handle_alert("KRAKEN", guid, title, ts_ms, link or "https://blog.kraken.com")

            time.sleep(KRAKEN_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[KRAKEN ERROR] {e}")
            time.sleep(15)


# ----------------------------
# Telegram listener (/status + /stop for ringer)
# ----------------------------
def telegram_listener():
    offset = 0
    session = get_session()
    safe_log("[STATUS] Telegram Listener Started")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            resp = session.get(url, params={"offset": offset, "timeout": 30}, timeout=40)
            data = resp.json()

            if "result" in data:
                for update in data["result"]:
                    offset = update["update_id"] + 1
                    if "message" not in update or "text" not in update["message"]:
                        continue

                    if str(update["message"]["chat"]["id"]) != TELEGRAM_CHAT_ID:
                        continue

                    text = update["message"]["text"].lower().strip()

                    if text in ["/status", "status"]:
                        msg = (
                            f"SYSTEM REPORT\n"
                            f"State: RUNNING\n"
                            f"Time: {get_utc3_time()} (UTC+3)\n"
                            f"Filter: <= 1 hour\n"
                            f"Gate: Binance USD-M OR MEXC Futures\n"
                            f"Ringer: {ringer_status()}\n"
                            f"Commands: /status, /stop"
                        )
                        send_telegram_msg(msg, parse_mode=None)

                    elif text in ["/stop", "stop"]:
                        # Stops ONLY the current ringer cycle; future matches can ring again
                        stop_ringer()
                        send_telegram_msg("[SYSTEM] Reminder stopped.", parse_mode=None)

        except Exception:
            time.sleep(5)


# ----------------------------
# Binance public WS
# ----------------------------
def run_public_ws():
    def on_public_msg(ws, message):
        try:
            data = json.loads(message)
            if "data" not in data:
                return
            item = data["data"]
            if isinstance(item, list) and item:
                item = item[0]
            if not isinstance(item, dict):
                return

            title = item.get("title") or ""
            code = str(item.get("code", "")) or title
            ts = int(item.get("releaseDate", 0) or 0)
            url = f"https://www.binance.com/en/support/announcement/{item.get('code')}" if item.get("code") else "https://www.binance.com/en/support/announcement"
            if title:
                handle_alert("BINANCE", f"PUB:{code}", title, ts, url)
        except Exception:
            pass

    safe_log("[STATUS] Public WebSocket Started (Layer 2)")
    while True:
        try:
            ws = websocket.WebSocketApp(BINANCE_PUBLIC_WS, on_message=on_public_msg)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            time.sleep(5)


# ----------------------------
# Binance SAPI WS (main loop)
# ----------------------------
def get_sapi_url():
    base_url = "wss://api.binance.com/sapi/wss"
    params = {
        "random": str(uuid.uuid4()).replace("-", ""),
        "recvWindow": 5000,
        "timestamp": int(time.time() * 1000),
        "topic": BINANCE_SAPI_TOPIC,
    }
    sorted_params = sorted(params.items())
    query_string = urlencode(sorted_params)
    signature = hmac.new(
        BINANCE_SECRET_KEY.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{base_url}?{query_string}&signature={signature}"


def on_sapi_msg(ws, message):
    try:
        data = json.loads(message)
        if "ping" in data:
            ws.send(json.dumps({"pong": data["ping"]}))
            return

        payload = data.get("data", data)
        if not isinstance(payload, dict):
            return

        title = payload.get("title") or ""
        code = str(payload.get("code", "")) or title
        ts = int(payload.get("publishTime", 0) or 0)
        url = f"https://www.binance.com/en/support/announcement/{payload.get('code')}" if payload.get("code") else "https://www.binance.com/en/support/announcement"
        if title:
            handle_alert("BINANCE", f"SAPI:{code}", title, ts, url)

    except Exception:
        pass


def on_open(ws):
    safe_log("[STATUS] Connected to SAPI Stream (Layer 1)")
    send_telegram_msg(
        "[SYSTEM] Bot Online (Cloud)\n"
        "Monitoring: Binance + Bybit + XT + KuCoin + Gate + WEEX + BingX + Bitget + Kraken + MEXC.\n"
        "Filter: <= 1 hour\n"
        "Gate: Binance USD-M OR MEXC Futures\n"
        "Commands: /status, /stop",
        parse_mode=None,
    )


# ----------------------------
# Tests (CLI flags)
# ----------------------------
def run_tests(args):
    s = get_session()

    if args.test_futures:
        for t in args.test_futures:
            t = t.strip().upper()
            try:
                bq = binance_usdm_quotes_for(t, s)
            except Exception:
                bq = set()
            try:
                ms = mexc_symbols_for(t, s)
            except Exception:
                ms = set()
            print(f"{t}: Binance USD-M={'YES' if bq else 'NO'} quotes={sorted(list(bq))} | MEXC Futures={'YES' if ms else 'NO'} symbols={sorted(list(ms))}")
        sys.exit(0)

    title = None
    source = None
    if args.test_title:
        title = args.test_title.strip()
        source = args.test_title_source.strip().upper()
    elif args.test_bybit_title:
        title = args.test_bybit_title.strip()
        source = "BYBIT"

    if title:
        tickers = extract_tickers(title)
        print(f"SOURCE: {source}")
        print(f"TITLE:  {title}")
        print(f"TICKERS: {tickers}")
        for t in tickers:
            if t in TOPIC_CRYPTOS:
                continue
            allowed, bin_ok, mex_ok, bq, ms = passes_futures_gate(t, s)
            print(f"- {t}: allowed={allowed}  binance_usdm={bin_ok} quotes={sorted(list(bq))}  mexc_futures={mex_ok} symbols={sorted(list(ms))}")
        sys.exit(0)


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-futures", nargs="+", help="Test futures gate (e.g. --test-futures BTC DOGE ABC)")
    parser.add_argument("--test-title", type=str, help='Test ticker extraction + gate (e.g. --test-title "Listing (ABC)" )')
    parser.add_argument("--test-title-source", type=str, default="XT", help="Label only (default XT)")
    parser.add_argument("--test-bybit-title", type=str, help='Shortcut for testing Bybit-like titles (e.g. --test-bybit-title "New Listing (ABC)" )')
    args, _ = parser.parse_known_args()

    run_tests(args)

    # Start Telegram sender FIRST
    threading.Thread(target=telegram_sender, daemon=True).start()

    send_telegram_msg(
        f"[SYSTEM] Bot starting...\n"
        f"Time: {get_utc3_time()} (UTC+3)\n"
        f"Filter: <= 1 hour\n"
        f"Gate: Binance USD-M OR MEXC Futures\n"
        f"Commands: /status, /stop",
        parse_mode=None,
    )

    # Start background threads
    threading.Thread(target=telegram_listener, daemon=True).start()

    threading.Thread(target=monitor_bybit, daemon=True).start()
    threading.Thread(target=monitor_xt, daemon=True).start()
    threading.Thread(target=monitor_kucoin, daemon=True).start()

    threading.Thread(target=monitor_gate, daemon=True).start()
    threading.Thread(target=monitor_weex, daemon=True).start()
    threading.Thread(target=monitor_bingx, daemon=True).start()
    threading.Thread(target=monitor_bitget, daemon=True).start()
    threading.Thread(target=monitor_kraken, daemon=True).start()
    threading.Thread(target=monitor_mexc, daemon=True).start()

    threading.Thread(target=monitor_binance_poll, daemon=True).start()
    threading.Thread(target=run_public_ws, daemon=True).start()

    # Main loop: Binance SAPI WS
    while True:
        try:
            ws_url = get_sapi_url()
            headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
            ws = websocket.WebSocketApp(
                ws_url,
                header=headers,
                on_open=on_open,
                on_message=on_sapi_msg,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            safe_log(f"[SAPI RESTART] {e}")
            time.sleep(5)
