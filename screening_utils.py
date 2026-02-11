from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

import requests

FAPI_EXCHANGEINFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_CACHE_TTL_SEC = 600

MEXC_CONTRACT_DETAIL_URL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUTURES_CACHE_TTL_SEC = 600

PAIR_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH", "BNB", "EUR", "GBP", "TRY")

IGNORE_WORDS = {
    "THE",
    "ON",
    "TOKEN",
    "LISTING",
    "LISTINGS",
    "FUTURES",
    "PERPETUAL",
    "PERP",
    "CONTRACT",
    "USD",
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDK",
    "USDⓈ",
    "USD-M",
    "MARGIN",
    "SPOT",
    "TRADING",
    "TRADES",
    "WILL",
    "LAUNCH",
    "LAUNCHES",
    "OPEN",
    "OPENING",
    "NEW",
    "ADD",
    "ADDS",
    "LIST",
    "PAIR",
    "PAIRS",
    "MARKET",
    "ANNOUNCEMENT",
    "SUPPORT",
    "SUPPORTS",
    "WITH",
    "AND",
    "FOR",
    "FROM",
    "THIS",
    "THAT",
    "YOUR",
    "OUR",
    "WEEK",
    "DAY",
    "HOUR",
    "PER",
    "MEXC",
    "BINANCE",
    "BYBIT",
    "KUCOIN",
    "GATE",
    "KRAKEN",
    "BITGET",
    "XT",
    "FUTURE",
    "SWAP",
    "INNOVATION",
    "ZONE",
    "PREMARKET",
    "PRE",
    "MARKET",
    "UTC",
    "UP",
    "DOWN",
    "IN",
    "AT",
    "TO",
    "OF",
    "A",
    "AN",
    "IS",
    "ARE",
    "BE",
    "ITS",
    "AS",
    "ONCHAIN",
    "COM",
    "COMPLETION",
    "CHANGES",
    "UPDATE",
    "UPDATED",
    "UPDATES",
    "NOTICE",
    "ANNOUNCED",
    "ANNOUNCE",
    "PLEASE",
    "READ",
    "ONLY",
    "NOW",
    "LIVE",
    "AVAILABLE",
    "OPENED",
    "OPENED",
    "LISTED",
    "WALLET",
    "DEPOSIT",
    "WITHDRAWAL",
    "ALL",
    "MET",
    "ID",
    "AI",
    "PEOPLE",
    "BANK",
    "XAU",
    "XAG",
    "EUR",
    "AUD",
    "GBP",
    "ORDER",
    "TAG",
    "WIN",
}

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
}

LOGGER = logging.getLogger(__name__)

_bin_fut_lock = threading.Lock()
_bin_fut_loaded_at = 0
_bin_base_to_quotes: Dict[str, Set[str]] = {}

_mexc_fut_lock = threading.Lock()
_mexc_fut_loaded_at = 0
_mexc_base_to_symbols: Dict[str, Set[str]] = {}

_extract_log_lock = threading.Lock()
_extract_log_count = 0
_EXTRACT_LOG_LIMIT = 10
_PAIR_PATTERN = re.compile(
    r"([A-Z0-9]{2,15})(?:\\s*[-_/ ]+\\s*|)(USDT|USDC|USD|BTC|ETH|BNB)"
)
_PAREN_TICKER_PATTERN = re.compile(r"\\(([A-Z0-9]{2,15})\\)")
_FALLBACK_TICKER_PATTERN = re.compile(r"\b[A-Z0-9]{2,15}\b")
_FALLBACK_HINT_PATTERNS = [
    re.compile(r"\bSUPPORTS\s+([A-Z0-9]{2,15})\b"),
    re.compile(r"\bLISTS?\s+([A-Z0-9]{2,15})\b"),
    re.compile(r"\bADDS?\s+([A-Z0-9]{2,15})\b"),
]
_FALLBACK_STOPWORDS = {
    "GATE",
    "NOW",
    "SUPPORTS",
    "FOR",
    "FUTURES",
    "TRADING",
    "SPOT",
    "MARGIN",
    "LOANS",
    "BOTS",
    "COPY",
    "CONVERT",
    "AUTO",
    "INVEST",
    "PERP",
    "DEX",
    "AND",
    "WILL",
    "LIST",
    "LAUNCH",
    "ADD",
    "NEW",
    "AVAILABLE",
    "OPEN",
    "STARTS",
    "START",
    "TRADE",
    "MARKET",
    "USDT",
    "USDC",
    "USD",
    "BTC",
    "ETH",
    "BNB",
}


def get_session(use_cache: bool = True, clear_cache: bool = False) -> requests.Session:
    if use_cache:
        import requests_cache

        session: requests.Session = requests_cache.CachedSession(
            cache_name="http_cache",
            backend="sqlite",
            expire_after=10800,
        )
        if clear_cache and isinstance(session, requests_cache.CachedSession):
            session.cache.clear()
    else:
        session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "mexc-futures-listing-analyzer/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return session


def get_plain_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=2)
    session.mount("https://", adapter)
    return session


def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


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


def mark_seen(source: str, uniq: str) -> bool:
    with SEEN_LOCK:
        if uniq in SEEN[source]:
            return False
        SEEN[source].add(uniq)
        return True


def extract_tickers(text: str) -> List[str]:
    upper = text.upper()
    matches = _PAIR_PATTERN.findall(upper)
    paren_matches = _PAREN_TICKER_PATTERN.findall(upper)

    bases: Set[str] = set()
    for base, quote in matches:
        if quote in PAIR_QUOTES and base:
            bases.add(base)
    for base in paren_matches:
        if base:
            bases.add(base)

    if not bases:
        for pattern in _FALLBACK_HINT_PATTERNS:
            bases.update(pattern.findall(upper))
        bases.update(_FALLBACK_TICKER_PATTERN.findall(upper))

    filtered = [
        base
        for base in bases
        if base not in IGNORE_WORDS and base not in _FALLBACK_STOPWORDS
    ]
    result = sorted(set(filtered))

    global _extract_log_count
    with _extract_log_lock:
        if _extract_log_count < _EXTRACT_LOG_LIMIT:
            LOGGER.info(
                "extract_tickers pattern=%s raw=%s upper=%s matches=%s result=%s",
                _PAIR_PATTERN.pattern,
                text,
                upper,
                matches,
                result,
            )
            _extract_log_count += 1

    return result


if __name__ == "__main__":
    import unittest

    class ExtractTickerTests(unittest.TestCase):
        def test_extract_concatenated_pairs(self):
            title = "XT.COM Futures Will Launch USDT-M FRAXUSDT and LITUSDT Perpetual Futures"
            self.assertEqual(extract_tickers(title), ["FRAX", "LIT"])

        def test_extract_single_pair(self):
            title = "Adjusting ... for MEUSDT Perpetual Contracts"
            self.assertEqual(extract_tickers(title), ["ME"])

        def test_extract_bitget_pair(self):
            title = "FUNUSDT now launched for futures trading and trading bots"
            self.assertEqual(extract_tickers(title), ["FUN"])

        def test_extract_supports_fallback(self):
            title = (
                "Gate Now Supports FRAX for Futures Trading, Gate Perp DEX, Margin Loans, Bots, Copy"
            )
            self.assertEqual(extract_tickers(title), ["FRAX"])

    unittest.main()


def _refresh_binance_futures_cache(session):
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


def binance_usdm_quotes_for(ticker: str, session) -> Set[str]:
    now = int(time.time())
    with _bin_fut_lock:
        if (now - _bin_fut_loaded_at) > BINANCE_FUTURES_CACHE_TTL_SEC or not _bin_base_to_quotes:
            _refresh_binance_futures_cache(session)
        return set(_bin_base_to_quotes.get(ticker, set()))


def _refresh_mexc_futures_cache(session):
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


def mexc_symbols_for(ticker: str, session) -> Set[str]:
    now = int(time.time())
    with _mexc_fut_lock:
        if (now - _mexc_fut_loaded_at) > MEXC_FUTURES_CACHE_TTL_SEC or not _mexc_base_to_symbols:
            _refresh_mexc_futures_cache(session)
        return set(_mexc_base_to_symbols.get(ticker, set()))


def passes_futures_gate(ticker: str, session) -> Tuple[bool, bool, bool, Set[str], Set[str]]:
    bin_quotes = set()
    mexc_syms = set()
    try:
        bin_quotes = binance_usdm_quotes_for(ticker, session)
    except Exception:
        bin_quotes = set()

    try:
        mexc_syms = mexc_symbols_for(ticker, session)
    except Exception:
        mexc_syms = set()

    bin_ok = len(bin_quotes) > 0
    mexc_ok = len(mexc_syms) > 0
    return (bin_ok or mexc_ok), bin_ok, mexc_ok, bin_quotes, mexc_syms


def gate_fetch_listing_ids(html_text: str) -> List[str]:
    ids = re.findall(r'href="/announcements/article/(\d+)"', html_text)
    out, seen = [], set()
    for item in ids:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def mexc_extract_announcement_paths(html_text: str) -> List[str]:
    paths = re.findall(r'href="(/announcements/[^"]+)"', html_text)
    paths += re.findall(r'href="(/support/articles/\d+[^"]*)"', html_text)

    out, seen = [], set()
    for path in paths:
        clean = path.split("?")[0]
        if clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out
