from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type

LOGGER = logging.getLogger(__name__)

_BINANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.binance.com/en/support/announcement",
    "clienttype": "web",
    "Accept": "application/json, text/plain, */*",
}


def _fetch_cms_articles(session, cutoff_ts: float) -> List[Announcement]:
    cms_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    announcements: List[Announcement] = []
    page = 1
    max_pages = 20
    while page <= max_pages:
        params = {"type": 1, "pageNo": page, "pageSize": 50}
        LOGGER.info("Binance CMS url=%s params=%s", cms_url, params)
        response = session.get(cms_url, params=params, headers=_BINANCE_HEADERS, timeout=20)
        LOGGER.info(
            "Binance CMS response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Binance CMS response status=%s blocked_or_error", response.status_code)
            break
        response.raise_for_status()
        cms_data = response.json()
        catalogs = cms_data.get("data", {}).get("catalogs", [])
        page_had_items = False
        oldest_on_page_ts = None
        for catalog in catalogs:
            for item in catalog.get("articles", []):
                title = (item.get("title") or "").strip()
                code = item.get("code")
                timestamp = item.get("releaseDate")
                if not title or not code or not timestamp:
                    continue
                page_had_items = True
                published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
                pub_ts = published.timestamp()
                if oldest_on_page_ts is None or pub_ts < oldest_on_page_ts:
                    oldest_on_page_ts = pub_ts
                if pub_ts < cutoff_ts:
                    continue
                url = f"https://www.binance.com/en/support/announcement/{code}"
                market_type = infer_market_type(title, default="spot")
                tickers = extract_tickers(title)
                announcements.append(
                    Announcement(
                        source_exchange="Binance",
                        title=title,
                        published_at_utc=published,
                        launch_at_utc=None,
                        url=url,
                        listing_type_guess=guess_listing_type(title),
                        market_type=market_type,
                        tickers=tickers,
                        body="",
                    )
                )
        if not page_had_items:
            break
        if oldest_on_page_ts is not None and oldest_on_page_ts < cutoff_ts:
            LOGGER.info("Binance CMS page=%s oldest article past cutoff, stopping pagination", page)
            break
        page += 1
    LOGGER.info("Binance CMS fetched %s announcements across %s pages", len(announcements), page)
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    announcements = _fetch_cms_articles(session, cutoff)
    if not announcements:
        LOGGER.warning("Binance adapter produced 0 items after fallback attempts")
    return announcements
