from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type

LOGGER = logging.getLogger(__name__)


def _extract_type_tag(item: dict) -> Tuple[Optional[str], Optional[str]]:
    type_info = item.get("type") or {}
    tag_info = item.get("tag") or {}
    type_key = type_info.get("key") or type_info.get("title")
    tag_key = tag_info.get("key") or tag_info.get("title")
    return type_key, tag_key


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.bybit.com/v5/announcements/index"
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    announcements: List[Announcement] = []
    type_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    fetched_pages = 0
    total_items = 0
    items_in_window = 0
    items_after_filter = 0

    page = 1
    selected_type = None
    selected_tag = None
    while True:
        params = {"locale": "en-US", "limit": 50, "page": page}
        if selected_type:
            params["type"] = selected_type
        if selected_tag:
            params["tag"] = selected_tag
        response = session.get(url, params=params, timeout=20)
        LOGGER.info("Bybit request url=%s params=%s", url, params)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Bybit response status=%s blocked_or_error", response.status_code)
        LOGGER.info(
            "Bybit response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        response.raise_for_status()
        data = response.json()
        ret_code = data.get("retCode")
        ret_msg = data.get("retMsg")
        LOGGER.info("Bybit retCode=%s retMsg=%s", ret_code, ret_msg)
        if ret_code not in (0, "0", None):
            break

        items = data.get("result", {}).get("list", []) or []
        if not items:
            break

        fetched_pages += 1
        total_items += len(items)
        for item in items:
            type_key, tag_key = _extract_type_tag(item)
            if type_key:
                type_counts[type_key] += 1
            if tag_key:
                tag_counts[tag_key] += 1

            timestamp = item.get("dateTimestamp") or item.get("date")
            if not timestamp:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
            if published.timestamp() < cutoff:
                continue
            items_in_window += 1
            title = item.get("title", "")
            body = item.get("summary", "") or item.get("content", "")
            url_value = item.get("url", "")
            tickers = extract_tickers(f"{title} {body}")
            market_type = infer_market_type(f"{title} {body}", default="futures")
            LOGGER.info(
                "Bybit kept publishTime=%s type=%s tag=%s title=%s tickers=%s",
                published,
                type_key,
                tag_key,
                title,
                tickers,
            )
            announcements.append(
                Announcement(
                    source_exchange="Bybit",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url_value,
                    listing_type_guess=guess_listing_type(title),
                    market_type=market_type,
                    tickers=tickers,
                    body=body,
                )
            )
        if page == 1:
            if type_counts:
                LOGGER.info("Bybit type distribution=%s", dict(type_counts.most_common(10)))
            if tag_counts:
                LOGGER.info("Bybit tag distribution=%s", dict(tag_counts.most_common(10)))
            for key in list(type_counts.keys()):
                if "deriv" in key.lower() or "contract" in key.lower():
                    selected_type = key
                    break
            for key in list(tag_counts.keys()):
                if "perp" in key.lower() or "futures" in key.lower():
                    selected_tag = key
                    break
        if page >= 10:
            break
        page += 1

    items_after_filter = len(announcements)
    LOGGER.info(
        "Bybit fetched_pages=%s total_items=%s items_in_window=%s items_after_listing_filter=%s",
        fetched_pages,
        total_items,
        items_in_window,
        items_after_filter,
    )
    for item in announcements[:10]:
        LOGGER.info(
            "Bybit kept publishTime=%s title=%s tickers=%s",
            item.published_at_utc,
            item.title,
            item.tickers,
        )
    return announcements
