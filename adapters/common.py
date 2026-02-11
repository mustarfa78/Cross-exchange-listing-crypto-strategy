from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from dateutil import parser

from screening_utils import extract_tickers


@dataclass(frozen=True)
class Announcement:
    source_exchange: str
    title: str
    published_at_utc: datetime
    launch_at_utc: Optional[datetime]
    url: str
    listing_type_guess: str
    market_type: str
    tickers: List[str]
    body: str


FUTURES_KEYWORDS = (
    "futures",
    "perpetual",
    "perp",
    "premarket",
    "derivatives",
    "contract",
    "swap",
    "innovation",
)

SPOT_LISTING_KEYWORDS = (
    "will list",
    "listing",
    "listed",
    "spot trading",
    "available to trade",
    "available for trading",
    "trade starts",
    "adds",
    "new listing",
    "asset listing",
    "now available",
    "trading starts",
    "trading begins",
    "opens trading",
    "new asset",
    "introducing",
)

def guess_listing_type(title: str) -> str:
    lowered = title.lower()
    if "premarket" in lowered:
        return "premarket"
    if "perpetual" in lowered or "perp" in lowered:
        return "perpetual"
    if "innovation" in lowered:
        return "innovation"
    if "futures" in lowered or "contract" in lowered or "swap" in lowered:
        return "futures"
    if spot_keyword_match(lowered):
        return "spot"
    return "unknown"


def is_futures_announcement(title: str, extra_keywords: Iterable[str] | None = None) -> bool:
    return futures_keyword_match(title, extra_keywords) is not None


def futures_keyword_match(title: str, extra_keywords: Iterable[str] | None = None) -> Optional[str]:
    lowered = title.lower()
    if extra_keywords:
        for keyword in extra_keywords:
            if keyword in lowered:
                return keyword
    for keyword in FUTURES_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def spot_keyword_match(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword in SPOT_LISTING_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def infer_market_type(text: str, default: str = "futures") -> str:
    if futures_keyword_match(text):
        return "futures"
    if spot_keyword_match(text):
        return "spot"
    return default


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = parser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return ensure_utc(parsed)
