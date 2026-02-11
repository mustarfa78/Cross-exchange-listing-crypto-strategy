from __future__ import annotations

from datetime import datetime, timezone
from html import unescape
import re
from typing import List, Optional

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, infer_market_type
from http_client import get_json

LOGGER = logging.getLogger(__name__)

_KRAKEN_AVAILABLE_RE = re.compile(
    r"\b([A-Z0-9]{2,15})\b\s+IS\s+(?:NOW\s+)?AVAILABLE\s+FOR\s+TRADING\b",
    re.IGNORECASE,
)
_KRAKEN_TRADING_STARTS_RE = re.compile(
    r"TRADING\s+STARTS\s+FOR\s+([A-Z0-9]{2,15})",
    re.IGNORECASE,
)


def _extract_kraken_tickers(title: str) -> List[str]:
    upper = title.upper()
    for pattern in (_KRAKEN_AVAILABLE_RE, _KRAKEN_TRADING_STARTS_RE):
        match = pattern.search(upper)
        if match:
            return [match.group(1).upper()]
    return []


def _fetch_asset_listing_category_id(session) -> Optional[int]:
    category_url = "https://blog.kraken.com/wp-json/wp/v2/categories"
    try:
        categories = get_json(session, category_url, params={"slug": "asset-listings"})
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Kraken category fetch failed: %s", exc)
        return None
    if isinstance(categories, list) and categories:
        category_id = categories[0].get("id")
        if category_id:
            return int(category_id)
    return None


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    LOGGER.info("Kraken adapter using WP JSON feed for asset listings (spot)")
    feed_url = "https://blog.kraken.com/wp-json/wp/v2/posts"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    category_id = _fetch_asset_listing_category_id(session)
    titles_sample = []
    listing_pass = 0
    page = 1
    max_pages = 20
    while page <= max_pages:
        params = {"per_page": 50, "page": page}
        if category_id:
            params["categories"] = category_id
        try:
            posts = get_json(session, feed_url, params=params)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Kraken WP JSON fetch failed on page %s: %s", page, exc)
            break
        if not posts:
            break
        reached_cutoff = False
        for post in posts:
            title = (post.get("title") or {}).get("rendered", "") or ""
            title = unescape(title).strip()
            link = post.get("link", "")
            content = (post.get("content") or {}).get("rendered", "") or ""
            date_gmt = post.get("date_gmt")
            if not title or not link or not date_gmt:
                continue
            published = datetime.fromisoformat(date_gmt.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )
            if published.timestamp() < cutoff:
                reached_cutoff = True
                continue
            content_text = re.sub(r"<.*?>", " ", content)
            tickers = extract_tickers(f"{title} {content_text}")
            if not tickers:
                tickers = _extract_kraken_tickers(title)
            market_type = infer_market_type(title, default="spot")
            if len(titles_sample) < 10:
                titles_sample.append(title)
            if market_type == "spot":
                listing_pass += 1
            announcements.append(
                Announcement(
                    source_exchange="Kraken",
                    title=title.strip(),
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=link,
                    listing_type_guess=guess_listing_type(title),
                    market_type=market_type,
                    tickers=tickers,
                    body="",
                )
            )
        if reached_cutoff:
            LOGGER.info("Kraken page=%s reached cutoff date, stopping pagination", page)
            break
        if len(posts) < 50:
            break
        page += 1
    LOGGER.info(
        "Kraken fetched_count=%s listing_filter_pass_count=%s pages=%s sample_titles=%s",
        len(announcements),
        listing_pass,
        page,
        titles_sample,
    )
    return announcements
