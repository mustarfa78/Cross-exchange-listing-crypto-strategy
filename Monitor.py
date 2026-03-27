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
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from typing import Optional, Tuple, Set, Dict, List

# --- 🛡️ CRASH PREVENTION: FORCE UTF-8 ---
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# =========================
# CONFIGURATION
# =========================
BINANCE_API_KEY = ""
BINANCE_SECRET_KEY = ""

TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = "5642266820"

# =========================
# SETTINGS / ENDPOINTS
# =========================
# Binance announcements (all announcements)
BINANCE_SAPI_TOPIC = "com_announcement_en"
BINANCE_PUBLIC_WS = "wss://stream.binance.com:9443/ws/all_announcement"
BINANCE_POLL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

# Bybit announcements (monitored; gate applies)
BYBIT_URL = "https://api.bybit.com/v5/announcements/index"

# XT (Zendesk section)
XT_ZENDESK_SECTION_ID = "900000084163"
XT_ZENDESK_ARTICLES_API = f"https://xtsupport.zendesk.com/api/v2/help_center/en-us/sections/{XT_ZENDESK_SECTION_ID}/articles.json"

# KuCoin new listings API
KUCOIN_ANN_URL = "https://api.kucoin.com/api/v3/announcements"
KUCOIN_LANG = "en_US"
KUCOIN_ANN_TYPE = "new-listings"

# Bitget announcements API
BITGET_ANN_URL = "https://api.bitget.com/api/v2/public/annoucements"

# Kraken blog WP API
KRAKEN_WP_URL = "https://blog.kraken.com/wp-json/wp/v2/posts"

# Weex Zendesk
WEEX_ZENDESK_SEARCH = "https://weexsupport.zendesk.com/api/v2/help_center/articles.json"

# ✅ BingX Zendesk articles feed (more reliable than query=listing)
BINGX_ZENDESK_ARTICLES_API = "https://bingxservice.zendesk.com/api/v2/help_center/articles.json"

# Gate + MEXC (web pages)
GATE_NEWLISTED_URL = "https://www.gate.com/announcements/newlisted"
MEXC_NEWLISTINGS_URL = "https://www.mexc.com/announcements/new-listings"

# Upbit: backend JSON API (the notice page is a React SPA; use the raw API)
UPBIT_NOTICE_API = "https://api.upbit.com/v1/notices"

# Binance USDⓈ-M Futures exchangeInfo (public)
FAPI_EXCHANGEINFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_CACHE_TTL_SEC = 600  # 10 min

# MEXC futures (public)
MEXC_CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUTURES_CACHE_TTL_SEC = 600  # 10 min

# =========================
# POLL INTERVALS (polite)
# =========================
BYBIT_POLL_INTERVAL_SEC = 5
XT_POLL_INTERVAL_SEC = 8
KUCOIN_POLL_INTERVAL_SEC = 10
BITGET_POLL_INTERVAL_SEC = 10
KRAKEN_POLL_INTERVAL_SEC = 20
WEEX_POLL_INTERVAL_SEC = 15
BINGX_POLL_INTERVAL_SEC = 15  # a bit faster now that it's clean JSON

# Gate/MEXC webpage polling — keep slow to reduce bans
GATE_POLL_INTERVAL_SEC = 25
MEXC_POLL_INTERVAL_SEC = 25

# Upbit notice page — slightly slower since it's a consumer-facing site
UPBIT_POLL_INTERVAL_SEC = 30

# Binance REST poller
BINANCE_REST_POLL_INTERVAL_SEC = 3

# =========================
# FILTERS / RULES
# =========================
MAX_AGE = timedelta(hours=1)  # <= 1 hour only

# ✅ Send a debug telegram when tickers are found but FAIL gate (Binance+MEXC)
SEND_SCREENING_FAILURES = True

IGNORE_WORDS = {
    "BINANCE", "BYBIT", "XT", "KUCOIN", "KCS", "WEEX", "BINGX", "GATE", "KRAKEN", "MEXC",
    "WILL", "SUPPORT", "THE", "AND", "FOR", "WITH", "YOUR", "FROM", "THIS", "THAT",
    "OPEN", "CLOSE", "DAILY", "WEEKLY", "MONTHLY", "YEAR", "EARN", "SPOT", "MARGIN",
    "FUTURES", "CRYPTO", "TOKEN", "COIN", "LIST", "LISTING", "DELIST", "MAINTENANCE",
    "NETWORK", "WALLET", "UPGRADE", "UPDATE", "TIME", "DATE", "NOW", "NEW", "ALL",
    "DEPOSIT", "TRADE", "COMPLETE", "ADDED", "ADD", "LAUNCH", "PROJECT", "REVIEW",
    "REMOVAL", "NOTICE", "PLAN", "OFFLINE", "PERPETUAL", "CONTRACT", "SWAP", "OPTIONS",
    "USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "EUR", "GBP", "TRY", "RUB", "AUD", "CAD", "KRW",
    "MAINNET", "INTEGRATION", "SERVICES", "TRADING", "PAIRS", "ROUNDS", "SHARE", "REWARDS",
    "WORTH", "CAMPAIGN", "COMPETITION", "UNLOCK", "MEGA", "SIMPLE", "LOCKED", "PRODUCTS",
    "VOTE", "USER", "USERS", "RULES", "TERMS", "PRIZE", "POOL", "DISTRIBUTION", "MARKET",
}

TOPIC_CRYPTOS = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "AVAX"}

# =========================
# THREAD-SAFE DEDUPE
# =========================
SEEN_LOCK = threading.Lock()
SEEN: Dict[str, Set[str]] = {
    "BINANCE": set(),
    "BYBIT": set(),
    "XT": set(),
    "KUCOIN": set(),
    "BITGET": set(),
    "KRAKEN": set(),
    "WEEX": set(),
    "BINGX": set(),
    "GATE": set(),
    "MEXC": set(),
    "UPBIT": set(),
}

# Extra early-dedupe keys (for Gate/MEXC fetching loops)
_ONCE_LOCK = threading.Lock()
_ONCE_KEYS: Set[str] = set()

# =========================
# TELEGRAM SEND QUEUE
# =========================
TELE_Q: "queue.Queue[Tuple[str, Optional[str]]]" = queue.Queue(maxsize=5000)

# =========================
# RINGER
# =========================
RINGER_MESSAGE = "IMPORTANT: Recent Match Found, please /stop to stop the reminder"
RINGER_INTERVAL_SEC = 2   # <--- EDIT THIS to 1, 2, 3... seconds
RINGER_MAX_SECONDS = 120

_ringer_lock = threading.Lock()
_ringer_until_ts = 0.0
_ringer_stop_event = threading.Event()
_ringer_thread: Optional[threading.Thread] = None

# =========================
# FUTURES CACHES
# =========================
_bin_fut_lock = threading.Lock()
_bin_fut_loaded_at = 0
_bin_base_to_quotes: Dict[str, Set[str]] = {}

_mexc_fut_lock = threading.Lock()
_mexc_fut_loaded_at = 0
_mexc_base_to_symbols: Dict[str, Set[str]] = {}

# =========================
# HELPERS
# =========================
def safe_log(message: str):
    try:
        clean_msg = message.encode("ascii", "ignore").decode("ascii")
        print(clean_msg, flush=True)
    except Exception:
        print("[LOG ERROR]", flush=True)

def get_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    return session

def get_plain_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=2)
    session.mount("https://", adapter)
    return session

def _seen_once(key: str) -> bool:
    with _ONCE_LOCK:
        if key in _ONCE_KEYS:
            return True
        _ONCE_KEYS.add(key)
        return False

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

                    if r.status_code in (400, 401, 403):
                        safe_log(f"[TELEGRAM HTTP {r.status_code}] {r.text[:200]}")
                        break

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

def get_utc3_time(timestamp_ms: Optional[int] = None) -> str:
    if timestamp_ms:
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    else:
        dt_utc = datetime.now(timezone.utc)
    return (dt_utc + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def normalize_epoch_to_ms(x) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, str):
            x = x.strip()
            if x.isdigit():
                x = int(x)
            else:
                return iso_to_ms(x)
        if isinstance(x, (int, float)):
            x = int(x)
            if x < 10_000_000_000:
                return x * 1000
            return x
    except Exception:
        return 0
    return 0

def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)

def _is_too_old(ts_ms: int) -> bool:
    if not ts_ms:
        return False
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return (datetime.now(timezone.utc) - dt) > MAX_AGE
    except Exception:
        return False

# ✅ also split pair-like tokens PROMPTUSDT -> PROMPT
PAIR_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH", "BNB", "EUR", "GBP", "TRY")

def extract_tickers(text: str) -> List[str]:
    # No numbers in screening: only A-Z tickers
    safe_text = re.sub(r"[^A-Za-z\s\(\)]", "", text)
    bracket_matches = re.findall(r"\(([A-Z]{2,15})\)", safe_text)
    loose_matches = re.findall(r"\b[A-Z]{2,12}\b", safe_text)
    all_potential = bracket_matches + loose_matches

    expanded: Set[str] = set()
    for raw in all_potential:
        t = raw.strip().upper()
        if not t:
            continue

        # Expand "PROMPTUSDT" -> "PROMPT"
        for q in PAIR_QUOTES:
            if t.endswith(q) and len(t) > len(q) + 1:
                expanded.add(t[:-len(q)])

        expanded.add(t)

    out = []
    for t in expanded:
        if t in IGNORE_WORDS:
            continue
        out.append(t)

    return sorted(list(set(out)))

# =========================
# RINGER
# =========================
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

# =========================
# FUTURES: BINANCE USD-M
# =========================
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
        if base and quote:
            base_to_quotes.setdefault(base, set()).add(quote)

    _bin_base_to_quotes = base_to_quotes
    _bin_fut_loaded_at = int(time.time())

def binance_usdm_quotes_for(ticker: str, session: requests.Session) -> Set[str]:
    now = int(time.time())
    with _bin_fut_lock:
        if (now - _bin_fut_loaded_at) > BINANCE_FUTURES_CACHE_TTL_SEC or not _bin_base_to_quotes:
            _refresh_binance_futures_cache(session)
        return set(_bin_base_to_quotes.get(ticker, set()))

# =========================
# FUTURES: MEXC
# =========================
def _refresh_mexc_futures_cache(session: requests.Session):
    global _mexc_fut_loaded_at, _mexc_base_to_symbols
    resp = session.get(MEXC_CONTRACT_DETAIL_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    base_to_syms: Dict[str, Set[str]] = {}
    items = data.get("data") or []
    if isinstance(items, dict):
        items = items.get("list") or items.get("items") or []
    if not isinstance(items, list):
        items = []

    for it in items:
        if not isinstance(it, dict):
            continue
        base = (it.get("baseCoin") or it.get("base") or "").upper()
        sym = str(it.get("symbol") or it.get("contractCode") or "").upper()

        status = it.get("status")
        if isinstance(status, str) and status.lower() in ("offline", "disabled", "suspend"):
            continue

        if base and sym:
            base_to_syms.setdefault(base, set()).add(sym)

    _mexc_base_to_symbols = base_to_syms
    _mexc_fut_loaded_at = int(time.time())

def mexc_symbols_for(ticker: str, session: requests.Session) -> Set[str]:
    now = int(time.time())
    with _mexc_fut_lock:
        if (now - _mexc_fut_loaded_at) > MEXC_FUTURES_CACHE_TTL_SEC or not _mexc_base_to_symbols:
            _refresh_mexc_futures_cache(session)
        return set(_mexc_base_to_symbols.get(ticker, set()))

# =========================
# GATE: BINANCE OR MEXC (FINAL)
# =========================
def passes_futures_gate(ticker: str, session_json: requests.Session) -> Tuple[bool, bool, bool, Set[str], Set[str]]:
    """
    Returns: (allowed, bin_ok, mexc_ok, bin_quotes, mexc_syms)
    allowed if Binance USD-M OR MEXC futures.
    """
    bin_quotes = set()
    mexc_syms = set()
    try:
        bin_quotes = binance_usdm_quotes_for(ticker, session_json)
    except Exception:
        bin_quotes = set()

    try:
        mexc_syms = mexc_symbols_for(ticker, session_json)
    except Exception:
        mexc_syms = set()

    bin_ok = len(bin_quotes) > 0
    mexc_ok = len(mexc_syms) > 0
    return (bin_ok or mexc_ok), bin_ok, mexc_ok, bin_quotes, mexc_syms

def format_token_flags(ticker: str, bin_ok: bool, mexc_ok: bool) -> str:
    b1 = "Binance USD-M ✅" if bin_ok else "Binance USD-M ❌"
    b2 = "MEXC Futures ✅" if mexc_ok else "MEXC Futures ❌"
    return f"<b>{html.escape(ticker)}</b> ({b1}, {b2})"

# =========================
# CENTRAL ALERT HANDLER
# =========================
def mark_seen(source: str, uniq: str) -> bool:
    with SEEN_LOCK:
        if uniq in SEEN[source]:
            return False
        SEEN[source].add(uniq)
        return True

def handle_alert(source: str, uniq: str, title: str, ts_ms: int, url: str):
    if not mark_seen(source, uniq):
        return

    # Log visibility even when too old (helps "missed announcements" troubleshooting)
    if ts_ms and _is_too_old(ts_ms):
        safe_log(f"[SKIP-OLD] {source}: {title}")
        return

    tickers = extract_tickers(title)
    candidates = [t for t in tickers if t not in TOPIC_CRYPTOS]
    if not candidates:
        return

    session_json = get_session()

    passed_parts = []
    failed_parts = []

    for t in candidates:
        allowed, bin_ok, mexc_ok, _, _ = passes_futures_gate(t, session_json)
        tag = format_token_flags(t, bin_ok, mexc_ok)
        if allowed:
            passed_parts.append(tag)
        else:
            failed_parts.append(tag)

    date_str = get_utc3_time(ts_ms) if ts_ms else get_utc3_time()
    safe_title = html.escape(title, quote=False)
    safe_url = html.escape(url, quote=True)

    if passed_parts:
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
        return

    # ✅ Screening failure message (for troubleshooting)
    safe_log(f"[GATE-FAIL] {source}: extracted={candidates} title={title}")

    if SEND_SCREENING_FAILURES:
        msg = (
            f"--- SCREENING FAILED ---\n"
            f"Source: {html.escape(source)}\n"
            f"Time: {html.escape(date_str)} (UTC+3)\n"
            f"Tickers found: {', '.join(failed_parts)}\n"
            f"Title: {safe_title}\n"
            f"<a href=\"{safe_url}\">[Link] Open Announcement</a>"
        )
        send_telegram_msg(msg, parse_mode="HTML")

# =========================
# BINANCE: WS + REST
# =========================
def monitor_binance_poll():
    safe_log("[STATUS] Binance Poller Started (Layer 3)")
    session = get_session()
    while True:
        try:
            resp = session.get(BINANCE_POLL_URL, params={"type": 1, "pageNo": 1, "pageSize": 10}, timeout=20)
            data = resp.json()

            all_articles = []
            if "data" in data and isinstance(data["data"], dict) and "catalogs" in data["data"]:
                for catalog in data["data"]["catalogs"]:
                    all_articles.extend(catalog.get("articles", []))

            for a in all_articles[:8]:
                title = a.get("title", "") or ""
                code = str(a.get("code", "") or "")
                ts = int(a.get("releaseDate", 0) or 0)
                if not code or not title:
                    continue
                url = f"https://www.binance.com/en/support/announcement/{code}"
                handle_alert("BINANCE", code, title, ts, url)

            time.sleep(BINANCE_REST_POLL_INTERVAL_SEC)
        except Exception:
            time.sleep(5)

def run_public_ws():
    def on_public_msg(ws, message):
        try:
            data = json.loads(message)
            item = data.get("data")
            if isinstance(item, list) and item:
                item = item[0]
            if isinstance(item, dict) and "title" in item:
                title = item.get("title", "") or ""
                code = str(item.get("code", "") or "")
                ts = int(item.get("releaseDate", 0) or 0)
                if title and code:
                    url = f"https://www.binance.com/en/support/announcement/{code}"
                    handle_alert("BINANCE", code, title, ts, url)
        except Exception:
            pass

    safe_log("[STATUS] Public WebSocket Started (Layer 2)")
    while True:
        try:
            ws = websocket.WebSocketApp(BINANCE_PUBLIC_WS, on_message=on_public_msg)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            time.sleep(5)

def get_sapi_url():
    base_url = "wss://api.binance.com/sapi/wss"
    params = {
        "random": str(uuid.uuid4()).replace("-", ""),
        "recvWindow": 5000,
        "timestamp": int(time.time() * 1000),
        "topic": BINANCE_SAPI_TOPIC,
    }
    query_string = urlencode(sorted(params.items()))
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
            title = payload.get("title", "") or ""
            code = str(payload.get("code", "") or "")
            ts = int(payload.get("publishTime", 0) or 0)
            if title and code:
                url = f"https://www.binance.com/en/support/announcement/{code}"
                handle_alert("BINANCE", code, title, ts, url)
    except Exception:
        pass

_sapi_announced = False  # Only send "Bot Online" Telegram once per process run

def on_open(ws):
    global _sapi_announced
    safe_log("[STATUS] Connected to SAPI Stream (Layer 1)")
    if not _sapi_announced:
        _sapi_announced = True
        send_telegram_msg(
            "[SYSTEM] Bot Online (Cloud)\n"
            "Monitoring: Binance + Bybit + XT + KuCoin + Bitget + Kraken + Weex + BingX + Gate + MEXC.\n"
            "Filter: <= 1 hour\n"
            "Gate: Binance USD-M OR MEXC Futures\n"
            "Commands: /status, /stop",
            parse_mode=None,
        )

# =========================
# BYBIT
# =========================
def monitor_bybit():
    safe_log("[STATUS] Bybit Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(BYBIT_URL, params={"locale": "en-US", "limit": 10, "type": "new_crypto"}, timeout=30)
            data = resp.json()
            if data.get("retCode") == 0:
                for a in data.get("result", {}).get("list", [])[:15]:
                    url = a.get("url") or ""
                    title = a.get("title") or ""
                    ts = normalize_epoch_to_ms(a.get("dateTimestamp"))
                    if not url or not title:
                        continue
                    handle_alert("BYBIT", url, title, ts, url)
            time.sleep(BYBIT_POLL_INTERVAL_SEC)
        except Exception as e:
            safe_log(f"[BYBIT ERROR] {e}")
            time.sleep(6)

# =========================
# ✅ XT (fixed: sort by updated_at desc + use updated_at timestamp)
# =========================
def monitor_xt():
    safe_log("[STATUS] XT Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(
                XT_ZENDESK_ARTICLES_API,
                params={"per_page": 50, "page": 1, "sort_by": "updated_at", "sort_order": "desc"},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            arts = data.get("articles", []) or []

            for a in arts[:40]:
                aid = str(a.get("id") or "")
                title = a.get("title") or ""
                url = a.get("html_url") or a.get("url") or ""
                # updated_at is what XT UI shows as "Updated"
                ts = normalize_epoch_to_ms(a.get("updated_at") or a.get("created_at"))

                if not aid or not title:
                    continue

                # XT is always included in their titles sometimes
                title = title.replace("(XT)", "").replace(" XT ", " ")

                handle_alert("XT", aid, title, ts, url or "https://xtsupport.zendesk.com")

            time.sleep(XT_POLL_INTERVAL_SEC + random.uniform(0.0, 2.0))
        except Exception as e:
            safe_log(f"[XT ERROR] {e}")
            time.sleep(10)

# =========================
# KUCOIN
# =========================
def monitor_kucoin():
    safe_log("[STATUS] KuCoin Poller Started")
    session = get_session()
    while True:
        try:
            params = {"currentPage": 1, "pageSize": 20, "annType": KUCOIN_ANN_TYPE, "lang": KUCOIN_LANG}
            resp = session.get(KUCOIN_ANN_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            items = (data.get("data") or {}).get("items", []) or []
            for it in items[:25]:
                ann_id = str(it.get("annId") or "")
                title = it.get("annTitle") or ""
                url = it.get("annUrl") or ""
                ts = normalize_epoch_to_ms(it.get("cTime"))
                if not ann_id or not title:
                    continue
                handle_alert("KUCOIN", ann_id, title, ts, url or "https://www.kucoin.com/announcement/new-listings")
            time.sleep(KUCOIN_POLL_INTERVAL_SEC)
        except Exception as e:
            safe_log(f"[KUCOIN ERROR] {e}")
            time.sleep(10)

# =========================
# BITGET
# =========================
def monitor_bitget():
    safe_log("[STATUS] Bitget Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(
                BITGET_ANN_URL,
                params={"annType": "coin_listings", "language": "en_US", "limit": "10"},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data") if isinstance(data, dict) else []
            if not isinstance(items, list):
                items = []

            for it in items[:30]:
                ann_id = str(it.get("annId") or it.get("id") or it.get("url") or "")
                title = it.get("annTitle") or it.get("title") or ""
                url = it.get("annUrl") or it.get("url") or "https://www.bitget.com/support"
                ts = normalize_epoch_to_ms(it.get("cTime") or it.get("time") or 0)
                if ann_id and title:
                    handle_alert("BITGET", ann_id, title, ts, url)

            time.sleep(BITGET_POLL_INTERVAL_SEC)
        except Exception as e:
            safe_log(f"[BITGET ERROR] {e}")
            time.sleep(10)

# =========================
# KRAKEN (WordPress)
# =========================
def monitor_kraken():
    safe_log("[STATUS] Kraken Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(KRAKEN_WP_URL, params={"per_page": 10}, timeout=20)
            resp.raise_for_status()
            posts = resp.json()
            if not isinstance(posts, list):
                posts = []

            for p in posts[:15]:
                pid = str(p.get("id") or "")
                title_obj = p.get("title") or {}
                title = (title_obj.get("rendered") or "") if isinstance(title_obj, dict) else str(title_obj)
                link = p.get("link") or "https://blog.kraken.com/category/product/asset-listings"
                content_obj = p.get("content") or {}
                content = (content_obj.get("rendered") or "") if isinstance(content_obj, dict) else ""
                dt = p.get("date_gmt") or p.get("modified_gmt") or ""
                ts = normalize_epoch_to_ms(dt)

                hay = (title + " " + content).lower()
                if ("asset" in hay and "listing" in hay) or ("trading starts" in hay) or ("asset listings" in hay):
                    title_clean = re.sub(r"<.*?>", "", title)
                    if pid and title_clean:
                        handle_alert("KRAKEN", pid, title_clean, ts, link)

            time.sleep(KRAKEN_POLL_INTERVAL_SEC)
        except Exception as e:
            safe_log(f"[KRAKEN ERROR] {e}")
            time.sleep(12)

# =========================
# WEEX (Zendesk)
# =========================
def monitor_weex():
    safe_log("[STATUS] Weex Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(WEEX_ZENDESK_SEARCH, params={"label_names": "new_listing", "per_page": 30}, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            arts = data.get("articles", []) or []
            for a in arts[:20]:
                aid = str(a.get("id") or "")
                title = a.get("title") or ""
                url = a.get("html_url") or a.get("url") or ""
                ts = normalize_epoch_to_ms(a.get("updated_at") or a.get("created_at"))
                if not aid or not title:
                    continue
                handle_alert("WEEX", aid, title, ts, url or "https://www.weex.com/wiki/new-listings")
            time.sleep(WEEX_POLL_INTERVAL_SEC)
        except Exception as e:
            safe_log(f"[WEEX ERROR] {e}")
            time.sleep(12)

# =========================
# ✅ BINGX (fixed: pull recent articles feed + keyword filter)
# =========================
BINGX_TITLE_KEYWORDS = re.compile(r"(list|listed|listing|adds|add|futures|spot|innovation|trading starts)", re.I)

def monitor_bingx():
    safe_log("[STATUS] BingX Poller Started")
    session = get_session()
    while True:
        try:
            resp = session.get(
                BINGX_ZENDESK_ARTICLES_API,
                params={"per_page": 60, "page": 1, "sort_by": "updated_at", "sort_order": "desc"},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            arts = data.get("articles", []) or []

            for a in arts[:50]:
                rid = str(a.get("id") or "")
                title = a.get("title") or ""
                url = a.get("html_url") or a.get("url") or "https://bingx.com"
                ts = normalize_epoch_to_ms(a.get("updated_at") or a.get("created_at"))

                if not rid or not title:
                    continue

                # Reduce noise: only titles likely to be listings/futures/spot
                if not BINGX_TITLE_KEYWORDS.search(title):
                    continue

                handle_alert("BINGX", rid, title, ts, url)

            time.sleep(BINGX_POLL_INTERVAL_SEC + random.uniform(0.0, 2.0))
        except Exception as e:
            safe_log(f"[BINGX ERROR] {e}")
            time.sleep(12)

# ============================================================
# Gate + MEXC webpage parsers (unchanged)
# ============================================================
def _gate_fetch_listing_ids(html_text: str) -> List[str]:
    ids = re.findall(r'href="/announcements/article/(\d+)"', html_text)
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

    m_title = re.search(r"<h1[^>]*>(.*?)</h1>", t, flags=re.S | re.I)
    if m_title:
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", m_title.group(1))).strip()
    else:
        m2 = re.search(r"<title[^>]*>(.*?)</title>", t, flags=re.S | re.I)
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", (m2.group(1) if m2 else ""))).strip()

    m_ts = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+UTC", t)
    ts_ms = 0
    if m_ts:
        dt = datetime.strptime(
            m_ts.group(1) + " " + m_ts.group(2), "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)

    return title, ts_ms, url

def monitor_gate():
    safe_log("[STATUS] Gate.com Poller Started")
    session = get_plain_session()
    cooldown = 0

    while True:
        try:
            if cooldown > 0:
                time.sleep(cooldown)
                cooldown = 0

            r = session.get(GATE_NEWLISTED_URL, timeout=20)
            r.raise_for_status()
            ids = _gate_fetch_listing_ids(r.text)

            new_fetches = 0
            for aid in ids[:20]:
                if _seen_once(f"GATE_ID:{aid}"):
                    continue
                if new_fetches >= 6:
                    break
                try:
                    title, ts_ms, url = _gate_fetch_article(session, aid)
                    if not title:
                        continue
                    handle_alert("GATE", aid, title, ts_ms, url)
                    new_fetches += 1
                except Exception:
                    continue

            time.sleep(GATE_POLL_INTERVAL_SEC + random.uniform(0.0, 3.0))

        except Exception as e:
            safe_log(f"[GATE ERROR] {e}")
            if "403" in str(e):
                cooldown = 60 + random.randint(0, 30)
            else:
                cooldown = 10
            time.sleep(5)

def _mexc_extract_announcement_paths(html_text: str) -> List[str]:
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

    m_title = re.search(r"<h1[^>]*>(.*?)</h1>", t, flags=re.S | re.I)
    if m_title:
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", m_title.group(1))).strip()
    else:
        m2 = re.search(r"<title[^>]*>(.*?)</title>", t, flags=re.S | re.I)
        title = re.sub(r"\s+", " ", re.sub(r"<.*?>", "", (m2.group(1) if m2 else ""))).strip()

    ts_ms = 0
    m_meta = re.search(r'article:published_time"\s+content="([^"]+)"', t)
    if m_meta:
        try:
            ts_ms = iso_to_ms(m_meta.group(1))
        except Exception:
            ts_ms = 0

    if not ts_ms:
        m_dt = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", t)
        if m_dt:
            dt = datetime.strptime(
                m_dt.group(1) + " " + m_dt.group(2), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)

    return title, ts_ms, url

def monitor_mexc():
    safe_log("[STATUS] MEXC Poller Started")
    session = get_plain_session()
    cooldown = 0

    while True:
        try:
            if cooldown > 0:
                time.sleep(cooldown)
                cooldown = 0

            r = session.get(MEXC_NEWLISTINGS_URL, timeout=20)
            r.raise_for_status()
            paths = _mexc_extract_announcement_paths(r.text)

            new_fetches = 0
            for path in paths[:25]:
                if _seen_once(f"MEXC_PATH:{path}"):
                    continue
                if new_fetches >= 6:
                    break
                try:
                    title, ts_ms, url = _mexc_fetch_article(session, path)
                    if not title:
                        continue
                    handle_alert("MEXC", path, title, ts_ms, url)
                    new_fetches += 1
                except Exception:
                    continue

            time.sleep(MEXC_POLL_INTERVAL_SEC + random.uniform(0.0, 3.0))

        except Exception as e:
            safe_log(f"[MEXC ERROR] {e}")
            if "403" in str(e):
                cooldown = 60 + random.randint(0, 30)
            else:
                cooldown = 10
            time.sleep(5)

# ============================================================
# Upbit JSON API monitor
# Only fire on: "Market Support for CoinName(TICKER) (KRW, BTC, USDT Market)"
# The notice page is a React SPA so we call the backing JSON API directly.
# Language is requested via Accept-Language header; Upbit returns English
# titles when the header is set to en-US.
# ============================================================
UPBIT_LISTING_RE = re.compile(
    r"Market\s+Support\s+for\s+.+?\([A-Z0-9]+\)\s*\(",
    re.IGNORECASE,
)

def monitor_upbit():
    safe_log("[STATUS] Upbit Poller Started")
    session = get_plain_session()
    headers = {"Accept-Language": "en-US,en;q=0.9"}
    cooldown = 0

    while True:
        try:
            if cooldown > 0:
                time.sleep(cooldown)
                cooldown = 0

            resp = session.get(
                UPBIT_NOTICE_API,
                params={"page": 1, "per_page": 20},
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()

            # Unwrap whichever nesting the API uses
            items: list = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                inner = data.get("data") or data
                if isinstance(inner, list):
                    items = inner
                elif isinstance(inner, dict):
                    items = (
                        inner.get("list")
                        or inner.get("items")
                        or inner.get("notices")
                        or []
                    )

            if not items:
                safe_log(
                    f"[UPBIT DEBUG] 0 notices — raw keys: "
                    f"{list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
                )

            for notice in items[:20]:
                if not isinstance(notice, dict):
                    continue

                nid = str(notice.get("id") or notice.get("notice_id") or "")
                title = (notice.get("title") or "").strip()
                ts_raw = notice.get("created_at") or notice.get("published_at") or 0
                url = f"https://upbit.com/service_center/notice?id={nid}&language=en"

                ts_ms = normalize_epoch_to_ms(ts_raw)

                if not nid or not title:
                    continue

                # Strict filter — only new-listing announcements
                if not UPBIT_LISTING_RE.search(title):
                    continue

                handle_alert("UPBIT", nid, title, ts_ms, url)

            time.sleep(UPBIT_POLL_INTERVAL_SEC + random.uniform(0.0, 3.0))

        except Exception as e:
            safe_log(f"[UPBIT ERROR] {e}")
            if "403" in str(e) or "404" in str(e):
                cooldown = 60 + random.randint(0, 30)
            else:
                cooldown = 10
            time.sleep(5)

# =========================
# TELEGRAM LISTENER (/status, /stop)
# =========================
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
                    msg = update.get("message") or {}
                    text = (msg.get("text") or "").lower().strip()
                    chat = msg.get("chat") or {}
                    if str(chat.get("id")) != str(TELEGRAM_CHAT_ID):
                        continue

                    if text in ("/status", "status"):
                        report = (
                            f"SYSTEM REPORT\n"
                            f"State: RUNNING\n"
                            f"Time: {get_utc3_time()} (UTC+3)\n"
                            f"Filter: <= 1 hour\n"
                            f"Gate: Binance USD-M OR MEXC Futures\n"
                            f"Screening fail msgs: {'ON' if SEND_SCREENING_FAILURES else 'OFF'}\n"
                            f"Ringer: {ringer_status()}\n"
                            f"Commands: /status, /stop"
                        )
                        send_telegram_msg(report, parse_mode=None)

                    elif text in ("/stop", "stop"):
                        stop_ringer()
                        send_telegram_msg("[SYSTEM] Reminder stopped.", parse_mode=None)

        except Exception:
            time.sleep(5)

# =========================
# BINANCE SAPI WS (Layer 1 — background, best-effort)
# =========================
def monitor_binance_sapi():
    """
    Attempt the unofficial Binance SAPI announcement WebSocket.
    Layers 2 (public WS) and 3 (REST poll) already cover Binance, so this
    is strictly additive. If the endpoint keeps rejecting us we back off to
    60 s between attempts so it doesn't flood the log.
    """
    while True:
        try:
            ws_url = get_sapi_url()
            headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
            ws = websocket.WebSocketApp(
                ws_url, header=headers, on_open=on_open, on_message=on_sapi_msg
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            safe_log("[SAPI RESTART] Connection dropped, retrying in 60s...")
        except Exception as e:
            safe_log(f"[SAPI RESTART] {e}")
        time.sleep(60)

# =========================
# MAIN + TEST FLAGS
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-futures", nargs="+", help="Test futures gate (e.g. --test-futures BTC DOGE XYZ)")
    parser.add_argument("--test-title", type=str, help='Test title extraction + futures gate (e.g. --test-title "Adds PROMPTUSDT for Futures" )')
    args, _ = parser.parse_known_args()

    if args.test_futures:
        s_json = get_session()
        for t in args.test_futures:
            t = t.strip().upper()
            allowed, bin_ok, mexc_ok, bq, ms = passes_futures_gate(t, s_json)
            print(f"{t}: allowed={allowed}  binance_usdm={bin_ok} quotes={sorted(bq)}  mexc_futures={mexc_ok} syms={sorted(ms)}")
        sys.exit(0)

    if args.test_title:
        s_json = get_session()
        title_test = args.test_title
        tks = extract_tickers(title_test)
        print(f"TITLE:  {title_test}")
        print(f"TICKERS: {tks}")
        for t in tks:
            if t in TOPIC_CRYPTOS:
                continue
            allowed, bin_ok, mexc_ok, bq, ms = passes_futures_gate(t, s_json)
            print(f"- {t}: allowed={allowed}  binance_usdm={bin_ok} quotes={sorted(bq)}  mexc_futures={mexc_ok} syms={sorted(ms)}")
        sys.exit(0)

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
    threading.Thread(target=monitor_bitget, daemon=True).start()
    threading.Thread(target=monitor_kraken, daemon=True).start()
    threading.Thread(target=monitor_weex, daemon=True).start()
    threading.Thread(target=monitor_bingx, daemon=True).start()

    threading.Thread(target=monitor_gate, daemon=True).start()
    threading.Thread(target=monitor_mexc, daemon=True).start()
    threading.Thread(target=monitor_upbit, daemon=True).start()

    threading.Thread(target=monitor_binance_poll, daemon=True).start()
    threading.Thread(target=run_public_ws, daemon=True).start()
    threading.Thread(target=monitor_binance_sapi, daemon=True).start()

    # Keep the main thread alive — all exchange monitors are daemon threads above.
    while True:
        time.sleep(3600)
