from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    api_url = "https://api.bitget.com/api/v2/public/annoucements"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    page = 1
    max_pages = 10
    total_logged = 0
    while page <= max_pages:
        params = {"annType": "coin_listings", "language": "en_US", "pageNo": page, "pageSize": 50}
        response = session.get(api_url, params=params, timeout=20)
        LOGGER.info("Bitget request url=%s params=%s", api_url, params)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Bitget response status=%s blocked_or_error", response.status_code)
            break
        LOGGER.info(
            "Bitget response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("data", [])
        if not items:
            break
        reached_cutoff = False
        for item in items:
            timestamp = item.get("annTime") or item.get("cTime")
            if timestamp is None:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
            if published.timestamp() < cutoff:
                reached_cutoff = True
                continue
            title = item.get("title", "") or item.get("annTitle", "")
            body = item.get("content", "") or item.get("summary", "") or item.get("annDesc", "")
            url = item.get("url", "") or item.get("annUrl", "")
            tickers = extract_tickers(f"{title} {body}")
            market_type = infer_market_type(f"{title} {body}", default="futures")
            if total_logged < 10:
                LOGGER.info(
                    "Bitget sample title=%s annType=%s annSubType=%s tickers=%s",
                    title,
                    item.get("annType"),
                    item.get("annSubType"),
                    tickers,
                )
                total_logged += 1
            announcements.append(
                Announcement(
                    source_exchange="Bitget",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url,
                    listing_type_guess=guess_listing_type(title),
                    market_type=market_type,
                    tickers=tickers,
                    body=body,
                )
            )
        if reached_cutoff:
            LOGGER.info("Bitget page=%s reached cutoff date, stopping pagination", page)
            break
        page += 1
    LOGGER.info("Bitget fetched %s announcements across %s pages", len(announcements), page)
    return announcements
