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
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Set, Dict

# --- 🛡️ CRASH PREVENTION: FORCE UTF-8 ---
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# --- CONFIGURATION ---
BINANCE_API_KEY = 
BINANCE_SECRET_KEY = 

TELEGRAM_TOKEN = 
TELEGRAM_CHAT_ID = 

# --- SETTINGS ---
BINANCE_SAPI_TOPIC = "com_announcement_en"
BINANCE_PUBLIC_WS = "wss://stream.binance.com:9443/ws/all_announcement"
BINANCE_POLL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BYBIT_URL = "https://api.bybit.com/v5/announcements/index"

# XT (Zendesk Help Center) - Tokens and Trading Pairs Listing section
XT_ZENDESK_SECTION_ID = "900000084163"
XT_ZENDESK_ARTICLES_API = f"https://xtsupport.zendesk.com/api/v2/help_center/en-us/sections/{XT_ZENDESK_SECTION_ID}/articles.json"
XT_POLL_INTERVAL_SEC = 6

# Upbit notices API (new listings)
UPBIT_NOTICES_URL = "https://api-manager.upbit.com/api/v1/notices"
UPBIT_POLL_INTERVAL_SEC = 10

# KuCoin announcements API (new listings)
KUCOIN_ANN_URL = "https://api.kucoin.com/api/v3/announcements"
KUCOIN_LANG = "en_US"
KUCOIN_ANN_TYPE = "new-listings"
KUCOIN_POLL_INTERVAL_SEC = 10

# Binance USDⓈ-M Futures exchangeInfo (public)
FAPI_EXCHANGEINFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
FUTURES_CACHE_TTL_SEC = 600  # 10 minutes cache

# Bybit Perp instruments (public)
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_PERP_CACHE_TTL_SEC = 600  # per-ticker cache

# --- CONTROL FLAGS ---
BOT_ACTIVE = True

# --- 1 HOUR FILTER ---
MAX_AGE = timedelta(hours=1)

# --- DEDUPE (THREAD-SAFE) ---
SEEN_CODES = set()
SEEN_LOCK = threading.Lock()

SEEN_XT_IDS = set()
XT_LOCK = threading.Lock()

SEEN_KUCOIN_IDS = set()
KUCOIN_LOCK = threading.Lock()

SEEN_UPBIT_IDS = set()
UPBIT_LOCK = threading.Lock()

# --- TELEGRAM SEND QUEUE ---
# items are (text, parse_mode_or_none)
TELE_Q: "queue.Queue[Tuple[str, Optional[str]]]" = queue.Queue(maxsize=5000)

# --- BINANCE FUTURES CACHE (THREAD-SAFE) ---
_futures_cache_lock = threading.Lock()
_futures_cache_loaded_at = 0
_futures_base_to_quotes: Dict[str, Set[str]] = {}

# --- BYBIT PERP CACHE (PER-TICKER) ---
_bybit_perp_lock = threading.Lock()
_bybit_perp_cache: Dict[str, Tuple[int, bool, Set[str]]] = {}  # ticker -> (checked_at, ok, symbols)

# --- RINGER (REMINDER SPAM) ---
RINGER_MESSAGE = "IMPORTANT: Recent Match Found, please /stop to stop the reminder"
RINGER_INTERVAL_SEC = 2   # <--- change to 1 if you want every 1s
RINGER_MAX_SECONDS = 120

_ringer_lock = threading.Lock()
_ringer_until_ts = 0.0
_ringer_stop_event = threading.Event()
_ringer_thread: Optional[threading.Thread] = None

# --- FILTERS ---
IGNORE_WORDS = {
    "XT", "KUCOIN", "KCS", "UPBIT",
    "BINANCE", "BYBIT", "WILL", "SUPPORT", "THE", "AND", "FOR", "WITH", "YOUR", "FROM", "THIS",
    "THAT", "OPEN", "CLOSE", "DAILY", "WEEKLY", "MONTHLY", "YEAR", "EARN", "SPOT",
    "MARGIN", "FUTURES", "CRYPTO", "TOKEN", "COIN", "LIST", "DELIST", "MAINTENANCE",
    "NETWORK", "WALLET", "UPGRADE", "UPDATE", "TIME", "DATE", "NOW", "NEW", "ALL",
    "DEPOSIT", "TRADE", "COMPLETE", "ADDED", "ADD", "LAUNCH", "PROJECT", "REVIEW",
    "REMOVAL", "NOTICE", "PLAN", "OFFLINE", "PERPETUAL", "CONTRACT", "SWAP", "OPTIONS",
    "USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "EUR", "GBP", "TRY", "RUB", "AUD", "CAD",
    "MAINNET", "INTEGRATION", "SERVICES", "TRADING", "PAIRS", "ROUNDS", "SHARE", "REWARDS",
    "WORTH", "CAMPAIGN", "COMPETITION", "UNLOCK", "MEGA", "SIMPLE", "LOCKED", "PRODUCTS",
    "VOTE", "USER", "USERS", "RULES", "TERMS", "PRIZE", "POOL", "DISTRIBUTION", "MARKET", "KRW"
}
TOPIC_CRYPTOS = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "AVAX"}


# ------------------------
# Helpers
# ------------------------
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
                    if r.status_code >= 400:
                        safe_log(f"[TELEGRAM HTTP {r.status_code}] {r.text}")

                    if r.status_code in (400, 401, 403):
                        break

                    if r.status_code == 429:
                        try:
                            j = r.json()
                            wait = int(j.get("parameters", {}).get("retry_after", 5))
                        except Exception:
                            wait = 5
                        time.sleep(min(max(wait, 1), 60))
                        continue

                    if r.status_code >= 500:
                        time.sleep(min(2 ** attempt, 30))
                        continue

                    if r.ok:
                        break

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


def _is_too_old(timestamp_ms: int) -> bool:
    try:
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return (datetime.now(timezone.utc) - dt) > MAX_AGE
    except Exception:
        return False


def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def extract_tickers(text: str):
    # No numbers in screening: only A-Z tickers
    safe_text = re.sub(r"[^A-Za-z\s\(\)]", "", text)
    bracket_matches = re.findall(r"\(([A-Z]{2,15})\)", safe_text)
    loose_matches = re.findall(r"\b[A-Z]{2,10}\b", safe_text)
    all_potential = bracket_matches + loose_matches
    return list(set([t for t in all_potential if t not in IGNORE_WORDS]))


# ------------------------
# RINGER
# ------------------------
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


# ------------------------
# Binance USDⓈ-M Futures availability (cached)
# ------------------------
def _refresh_futures_cache(session: requests.Session):
    global _futures_cache_loaded_at, _futures_base_to_quotes
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

    _futures_base_to_quotes = base_to_quotes
    _futures_cache_loaded_at = int(time.time())


def is_on_binance_usdm_futures(ticker: str, session: requests.Session) -> bool:
    now = int(time.time())
    with _futures_cache_lock:
        if (now - _futures_cache_loaded_at) > FUTURES_CACHE_TTL_SEC or not _futures_base_to_quotes:
            _refresh_futures_cache(session)
        return ticker in _futures_base_to_quotes and len(_futures_base_to_quotes[ticker]) > 0


def futures_quotes_for(ticker: str, session: requests.Session):
    now = int(time.time())
    with _futures_cache_lock:
        if (now - _futures_cache_loaded_at) > FUTURES_CACHE_TTL_SEC or not _futures_base_to_quotes:
            _refresh_futures_cache(session)
        return _futures_base_to_quotes.get(ticker, set())


# ------------------------
# Bybit Perpetual Futures (USDT-linear) availability (per-ticker cache)
# ------------------------
def bybit_usdt_perp_symbols(ticker: str, session: requests.Session) -> Tuple[bool, Set[str]]:
    """
    Checks if ticker has Bybit USDT-settled perpetual contracts.
    Uses GET /v5/market/instruments-info?category=linear&baseCoin=TICKER
    """
    ticker = ticker.upper().strip()
    now = int(time.time())

    with _bybit_perp_lock:
        cached = _bybit_perp_cache.get(ticker)
        if cached:
            checked_at, ok, symbols = cached
            if (now - checked_at) <= BYBIT_PERP_CACHE_TTL_SEC:
                return ok, set(symbols)

    ok = False
    symbols: Set[str] = set()

    try:
        params = {
            "category": "linear",
            "baseCoin": ticker,
            "limit": 200,
        }
        resp = session.get(BYBIT_INSTRUMENTS_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data.get("retCode") == 0:
            result = data.get("result") or {}
            items = result.get("list") or []
            for it in items:
                # Bybit note: querying baseCoin can return USDT/USDC/inverse; filter to USDT perpetual
                if (it.get("status") or "") != "Trading":
                    continue
                if (it.get("settleCoin") or "") != "USDT":
                    continue
                ct = (it.get("contractType") or "")
                if "Perpetual" not in ct:
                    continue
                sym = it.get("symbol")
                if sym:
                    symbols.add(sym)
            ok = len(symbols) > 0

    except Exception:
        ok = False
        symbols = set()

    with _bybit_perp_lock:
        _bybit_perp_cache[ticker] = (now, ok, set(symbols))

    return ok, set(symbols)


# ------------------------
# Gate logic
# ------------------------
def passes_futures_gate(source: str, ticker: str, session: requests.Session) -> Tuple[bool, bool, bool, Set[str]]:
    """
    Returns (allowed, binance_ok, bybit_ok, bybit_symbols)

    Rules:
    - For Bybit announcements source: REQUIRE Binance USD-M futures (even if Bybit perp exists).
      (matches your "Bybit-only shouldn't pass" rule)
    - For other non-Binance sources (XT/KuCoin): allow if Binance USD-M OR Bybit USDT Perp.
    """
    binance_ok = False
    bybit_ok = False
    bybit_syms: Set[str] = set()

    try:
        binance_ok = is_on_binance_usdm_futures(ticker, session)
    except Exception:
        binance_ok = False

    try:
        bybit_ok, bybit_syms = bybit_usdt_perp_symbols(ticker, session)
    except Exception:
        bybit_ok, bybit_syms = False, set()

    if source == "BYBIT_ANN":
        allowed = binance_ok
    else:
        allowed = binance_ok or bybit_ok

    return allowed, binance_ok, bybit_ok, bybit_syms


def format_token_flags(ticker: str, binance_ok: bool, bybit_ok: bool) -> str:
    b1 = "Binance USD-M ✅" if binance_ok else "Binance USD-M ❌"
    b2 = "Bybit Perp ✅" if bybit_ok else "Bybit Perp ❌"
    return f"<b>{html.escape(ticker)}</b> ({b1}, {b2})"


# ------------------------
# Central alert processor (Binance)
# ------------------------
def process_binance_alert(source, title, code, timestamp, url):
    with SEEN_LOCK:
        if code in SEEN_CODES:
            return
        SEEN_CODES.add(code)

    if _is_too_old(int(timestamp)):
        return

    tickers = extract_tickers(title)
    if any(t not in TOPIC_CRYPTOS for t in tickers):
        ticker_str = ", ".join(tickers)
        date_str = get_utc3_time(int(timestamp))

        safe_log(f"[MATCH] {source}: {ticker_str}")

        safe_title = html.escape(title, quote=False)
        safe_ticker = html.escape(ticker_str, quote=False)
        safe_url = html.escape(url, quote=True)

        msg = (
            f"*** BINANCE ALERT ***\n"
            f"Method: {html.escape(source)}\n"
            f"Time: {html.escape(date_str)} (UTC+3)\n"
            f"Token: <b>{safe_ticker}</b>\n"
            f"Title: {safe_title}\n"
            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
        )
        send_telegram_msg(msg, parse_mode="HTML")
        trigger_ringer()


# ------------------------
# Thread: Binance REST poller
# ------------------------
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

            for article in all_articles[:5]:
                ts = int(article.get("releaseDate", 0))
                code = str(article.get("code", ""))

                if ts and _is_too_old(ts):
                    with SEEN_LOCK:
                        SEEN_CODES.add(code)
                    continue

                process_binance_alert(
                    "REST Poller",
                    article.get("title", ""),
                    code,
                    ts,
                    f"https://www.binance.com/en/support/announcement/{article.get('code')}",
                )

            time.sleep(3)
        except Exception:
            time.sleep(5)


# ------------------------
# Thread: Bybit announcements poller
# ------------------------
def monitor_bybit():
    safe_log("[STATUS] Bybit Poller Started")
    session = get_session()
    seen_urls = set()

    while True:
        try:
            resp = session.get(
                BYBIT_URL,
                params={"locale": "en-US", "limit": 10, "type": "new_crypto"},
                timeout=30,
            )
            data = resp.json()

            if data.get("retCode") == 0:
                for article in data["result"]["list"]:
                    url = article.get("url")
                    if not url or url in seen_urls:
                        continue

                    ts = int(article.get("dateTimestamp", 0))
                    if ts and _is_too_old(ts):
                        seen_urls.add(url)
                        continue

                    title = article.get("title", "")
                    tickers = extract_tickers(title)

                    if any(t not in TOPIC_CRYPTOS for t in tickers):
                        passed_parts = []
                        for t in tickers:
                            allowed, bin_ok, byb_ok, _ = passes_futures_gate("BYBIT_ANN", t, session)
                            if allowed:
                                passed_parts.append(format_token_flags(t, bin_ok, byb_ok))

                        if not passed_parts:
                            safe_log(f"[SKIP] Bybit: no tickers passed (requires Binance USD-M). Extracted={tickers}")
                            seen_urls.add(url)
                            continue

                        date_str = get_utc3_time(ts)
                        safe_log("[MATCH] Bybit passed: " + ", ".join([re.sub(r"<.*?>", "", p) for p in passed_parts]))

                        safe_title = html.escape(title, quote=False)
                        safe_url = html.escape(url, quote=True)

                        msg = (
                            f"*** BYBIT ALERT ***\n"
                            f"Time: {html.escape(date_str)} (UTC+3)\n"
                            f"Token: {', '.join(passed_parts)}\n"
                            f"Title: {safe_title}\n"
                            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
                        )
                        send_telegram_msg(msg, parse_mode="HTML")
                        trigger_ringer()

                    seen_urls.add(url)

            time.sleep(5)
        except Exception:
            time.sleep(5)


# ------------------------
# XT: fetch latest listing articles via Zendesk API
# ------------------------
def fetch_xt_articles(session: requests.Session, per_page: int = 30):
    resp = session.get(XT_ZENDESK_ARTICLES_API, params={"per_page": per_page, "page": 1}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("articles", [])


def monitor_xt():
    safe_log("[STATUS] XT Poller Started")
    session = get_session()

    while True:
        try:
            articles = fetch_xt_articles(session, per_page=30)

            for a in articles[:15]:
                aid = str(a.get("id", ""))
                if not aid:
                    continue

                with XT_LOCK:
                    if aid in SEEN_XT_IDS:
                        continue

                title = a.get("title", "") or ""
                url = a.get("html_url") or (a.get("url") or "")
                created_at = a.get("created_at") or a.get("updated_at")
                if not created_at:
                    with XT_LOCK:
                        SEEN_XT_IDS.add(aid)
                    continue

                ts_ms = iso_to_ms(created_at)
                if _is_too_old(ts_ms):
                    with XT_LOCK:
                        SEEN_XT_IDS.add(aid)
                    continue

                tickers = [t for t in extract_tickers(title) if t != "XT"]

                if any(t not in TOPIC_CRYPTOS for t in tickers):
                    passed_parts = []
                    for t in tickers:
                        allowed, bin_ok, byb_ok, _ = passes_futures_gate("XT", t, session)
                        if allowed:
                            passed_parts.append(format_token_flags(t, bin_ok, byb_ok))

                    if passed_parts:
                        date_str = get_utc3_time(ts_ms)
                        safe_log("[MATCH] XT passed: " + ", ".join([re.sub(r"<.*?>", "", p) for p in passed_parts]))

                        safe_title = html.escape(title, quote=False)
                        safe_url = html.escape(url, quote=True)

                        msg = (
                            f"*** XT ALERT ***\n"
                            f"Time: {html.escape(date_str)} (UTC+3)\n"
                            f"Token: {', '.join(passed_parts)}\n"
                            f"Title: {safe_title}\n"
                            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
                        )
                        send_telegram_msg(msg, parse_mode="HTML")
                        trigger_ringer()
                    else:
                        safe_log(f"[SKIP] XT: no tickers passed. Extracted={tickers}")

                with XT_LOCK:
                    SEEN_XT_IDS.add(aid)

            time.sleep(XT_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[XT ERROR] {e}")
            time.sleep(8)


# ------------------------
# KuCoin: fetch new listings via KuCoin announcements API
# ------------------------
def fetch_kucoin_new_listings(session: requests.Session, page_size: int = 20):
    params = {
        "currentPage": 1,
        "pageSize": page_size,
        "annType": KUCOIN_ANN_TYPE,
        "lang": KUCOIN_LANG,
    }
    resp = session.get(KUCOIN_ANN_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return (data.get("data") or {}).get("items", [])


# ------------------------
# Upbit: fetch notices via Upbit API manager
# NOTE: URL https://api-manager.upbit.com/api/v1/notices needs verification via browser
#       DevTools on https://upbit.com/service_center/notice — update UPBIT_NOTICES_URL if
#       the actual endpoint differs. Field names (id, title, created_at, url) may also vary.
# ------------------------
def fetch_upbit_notices(session: requests.Session, per_page: int = 20):
    params = {"locale": "en", "page": 1, "per_page": per_page}
    resp = session.get(UPBIT_NOTICES_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        inner = data.get("data") or data
        return inner.get("list") or inner.get("notices") or []
    return []


def monitor_upbit():
    safe_log("[STATUS] Upbit Poller Started")
    session = get_session()

    while True:
        try:
            notices = fetch_upbit_notices(session, per_page=20)

            for notice in notices[:20]:
                notice_id = str(notice.get("id") or notice.get("notice_id") or "")
                if not notice_id:
                    continue

                with UPBIT_LOCK:
                    if notice_id in SEEN_UPBIT_IDS:
                        continue

                title = notice.get("title") or ""

                # Only process new-listing notices; Upbit English titles follow:
                # "Market Support for Xyz(XYZ) (KRW, BTC, USDT Market)"
                if "Market Support for" not in title:
                    with UPBIT_LOCK:
                        SEEN_UPBIT_IDS.add(notice_id)
                    continue

                raw_ts = notice.get("created_at") or notice.get("createdAt") or ""
                ts_ms = 0
                if raw_ts:
                    try:
                        ts_ms = iso_to_ms(str(raw_ts))
                    except Exception:
                        ts_ms = 0

                if ts_ms and _is_too_old(ts_ms):
                    with UPBIT_LOCK:
                        SEEN_UPBIT_IDS.add(notice_id)
                    continue

                url = notice.get("url") or f"https://upbit.com/service_center/notice?id={notice_id}"

                # Filter TOPIC_CRYPTOS so BTC from "(KRW, BTC, USDT Market)" doesn't appear as a match
                tickers = [t for t in extract_tickers(title) if t not in TOPIC_CRYPTOS]

                if tickers:
                    passed_parts = []
                    for t in tickers:
                        allowed, bin_ok, byb_ok, _ = passes_futures_gate("UPBIT", t, session)
                        if allowed:
                            passed_parts.append(format_token_flags(t, bin_ok, byb_ok))

                    if passed_parts:
                        date_str = get_utc3_time(ts_ms) if ts_ms else get_utc3_time()
                        safe_log("[MATCH] Upbit passed: " + ", ".join([re.sub(r"<.*?>", "", p) for p in passed_parts]))

                        safe_title = html.escape(title, quote=False)
                        safe_url = html.escape(url, quote=True)

                        msg = (
                            f"*** UPBIT ALERT ***\n"
                            f"Time: {html.escape(date_str)} (UTC+3)\n"
                            f"Token: {', '.join(passed_parts)}\n"
                            f"Title: {safe_title}\n"
                            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
                        )
                        send_telegram_msg(msg, parse_mode="HTML")
                        trigger_ringer()
                    else:
                        safe_log(f"[SKIP] Upbit: no tickers passed. Extracted={tickers}")

                with UPBIT_LOCK:
                    SEEN_UPBIT_IDS.add(notice_id)

            time.sleep(UPBIT_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[UPBIT ERROR] {e}")
            time.sleep(10)


def monitor_kucoin():
    safe_log("[STATUS] KuCoin Poller Started")
    session = get_session()

    while True:
        try:
            items = fetch_kucoin_new_listings(session, page_size=20)

            for it in items[:20]:
                ann_id = str(it.get("annId", ""))
                if not ann_id:
                    continue

                with KUCOIN_LOCK:
                    if ann_id in SEEN_KUCOIN_IDS:
                        continue

                title = it.get("annTitle", "") or ""
                url = it.get("annUrl", "") or ""
                ts_ms = int(it.get("cTime", 0) or 0)

                if ts_ms and _is_too_old(ts_ms):
                    with KUCOIN_LOCK:
                        SEEN_KUCOIN_IDS.add(ann_id)
                    continue

                tickers = [t for t in extract_tickers(title) if t not in ("KUCOIN", "KCS")]

                if any(t not in TOPIC_CRYPTOS for t in tickers):
                    passed_parts = []
                    for t in tickers:
                        allowed, bin_ok, byb_ok, _ = passes_futures_gate("KUCOIN", t, session)
                        if allowed:
                            passed_parts.append(format_token_flags(t, bin_ok, byb_ok))

                    if passed_parts:
                        date_str = get_utc3_time(ts_ms) if ts_ms else get_utc3_time()
                        safe_log("[MATCH] KuCoin passed: " + ", ".join([re.sub(r"<.*?>", "", p) for p in passed_parts]))

                        safe_title = html.escape(title, quote=False)
                        safe_url = html.escape(url, quote=True)

                        msg = (
                            f"*** KUCOIN ALERT ***\n"
                            f"Time: {html.escape(date_str)} (UTC+3)\n"
                            f"Token: {', '.join(passed_parts)}\n"
                            f"Title: {safe_title}\n"
                            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
                        )
                        send_telegram_msg(msg, parse_mode="HTML")
                        trigger_ringer()
                    else:
                        safe_log(f"[SKIP] KuCoin: no tickers passed. Extracted={tickers}")

                with KUCOIN_LOCK:
                    SEEN_KUCOIN_IDS.add(ann_id)

            time.sleep(KUCOIN_POLL_INTERVAL_SEC)

        except Exception as e:
            safe_log(f"[KUCOIN ERROR] {e}")
            time.sleep(10)


# ------------------------
# Thread: Telegram listener (/status + /stop for ringer)
# ------------------------
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
                    if "message" in update and "text" in update["message"]:
                        text = update["message"]["text"].lower().strip()

                        if str(update["message"]["chat"]["id"]) != TELEGRAM_CHAT_ID:
                            continue

                        if text in ["/status", "status"]:
                            msg = (
                                f"SYSTEM REPORT\n"
                                f"State: RUNNING\n"
                                f"Time: {get_utc3_time()} (UTC+3)\n"
                                f"Filter: <= 1 hour\n"
                                f"Gate: XT/KuCoin/Upbit allow Binance USD-M OR Bybit Perp\n"
                                f"Gate: Bybit announcements require Binance USD-M\n"
                                f"Ringer: {ringer_status()}\n"
                                f"Commands: /status, /stop"
                            )
                            send_telegram_msg(msg, parse_mode=None)

                        elif text in ["/stop", "stop"]:
                            stop_ringer()
                            send_telegram_msg("[SYSTEM] Reminder stopped.", parse_mode=None)

        except Exception:
            time.sleep(5)


# ------------------------
# Thread: Binance public WS
# ------------------------
def run_public_ws():
    def on_public_msg(ws, message):
        try:
            data = json.loads(message)
            if "data" in data:
                item = data["data"]
                if isinstance(item, list) and item:
                    item = item[0]
                if isinstance(item, dict) and "title" in item:
                    ts = int(item.get("releaseDate", 0))
                    code = str(item.get("code", ""))

                    if ts and _is_too_old(ts):
                        with SEEN_LOCK:
                            SEEN_CODES.add(code)
                        return

                    process_binance_alert(
                        "Public WebSocket",
                        item.get("title", ""),
                        code,
                        ts,
                        f"https://www.binance.com/en/support/announcement/{item.get('code')}",
                    )
        except Exception:
            pass

    safe_log("[STATUS] Public WebSocket Started (Layer 2)")
    while True:
        try:
            ws = websocket.WebSocketApp(BINANCE_PUBLIC_WS, on_message=on_public_msg)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            time.sleep(5)


# ------------------------
# Binance SAPI WS (main loop)
# ------------------------
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
        if isinstance(payload, dict) and "title" in payload:
            ts = int(payload.get("publishTime", 0))
            code = str(payload.get("code", ""))

            if ts and _is_too_old(ts):
                with SEEN_LOCK:
                    SEEN_CODES.add(code)
                return

            process_binance_alert(
                "SAPI Secure Stream",
                payload.get("title", ""),
                code,
                ts,
                f"https://www.binance.com/en/support/announcement/{payload.get('code')}",
            )
    except Exception:
        pass


def on_open(ws):
    safe_log("[STATUS] Connected to SAPI Stream (Layer 1)")
    send_telegram_msg(
        "[SYSTEM] Bot Online (Cloud)\n"
        "Monitoring Binance + Bybit + XT + KuCoin + Upbit.\n"
        "Commands: /status, /stop",
        parse_mode=None,
    )


# ------------------------
# Main + test flags
# ------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-futures", nargs="+", help="Test Binance USD-M futures (e.g. --test-futures BTC DOGE BREV)")
    parser.add_argument("--test-bybit-perp", nargs="+", help="Test Bybit USDT perpetual (e.g. --test-bybit-perp BTC DOGE)")
    parser.add_argument("--test-title", type=str, help='Test a title extraction and show gate results (e.g. --test-title "Listing (ABC)" )')
    parser.add_argument("--test-title-source", type=str, default="XT", help='Source for --test-title: XT | KUCOIN | BYBIT_ANN (default XT)')
    args, _ = parser.parse_known_args()

    if args.test_futures:
        s = get_session()
        for t in args.test_futures:
            t = t.strip().upper()
            try:
                quotes = futures_quotes_for(t, s)
                print(f"{t}: Binance USD-M {'YES' if quotes else 'NO'}  quotes={sorted(list(quotes))}")
            except Exception as e:
                print(f"{t}: ERROR {e}")
        sys.exit(0)

    if args.test_bybit_perp:
        s = get_session()
        for t in args.test_bybit_perp:
            t = t.strip().upper()
            ok, syms = bybit_usdt_perp_symbols(t, s)
            print(f"{t}: Bybit USDT Perp {'YES' if ok else 'NO'}  symbols={sorted(list(syms))}")
        sys.exit(0)

    if args.test_title:
        s = get_session()
        title = args.test_title.strip()
        source = args.test_title_source.strip().upper()
        tickers = extract_tickers(title)

        print(f"SOURCE: {source}")
        print(f"TITLE:  {title}")
        print(f"TICKERS: {tickers}")

        for t in tickers:
            allowed, bin_ok, byb_ok, byb_syms = passes_futures_gate(source, t, s)
            print(f"- {t}: allowed={allowed}  binance_usdm={bin_ok}  bybit_perp={byb_ok}  bybit_symbols={sorted(list(byb_syms))}")

        sys.exit(0)

    # Start Telegram sender FIRST
    threading.Thread(target=telegram_sender, daemon=True).start()

    send_telegram_msg(
        f"[SYSTEM] Bot starting...\n"
        f"Time: {get_utc3_time()} (UTC+3)\n"
        f"Filter: <= 1 hour\n"
        f"Gate: XT/KuCoin/Upbit allow Binance USD-M OR Bybit Perp\n"
        f"Gate: Bybit announcements require Binance USD-M\n"
        f"Commands: /status, /stop",
        parse_mode=None,
    )

    # Start Backgrounds
    threading.Thread(target=telegram_listener, daemon=True).start()
    threading.Thread(target=monitor_bybit, daemon=True).start()
    threading.Thread(target=monitor_xt, daemon=True).start()
    threading.Thread(target=monitor_kucoin, daemon=True).start()
    threading.Thread(target=monitor_upbit, daemon=True).start()
    threading.Thread(target=monitor_binance_poll, daemon=True).start()
    threading.Thread(target=run_public_ws, daemon=True).start()

    # Main Loop (SAPI)
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
