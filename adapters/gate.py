from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import (
    Announcement,
    extract_tickers,
    guess_listing_type,
    infer_market_type,
)

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_GATE_ARTICLE_ID_RE = re.compile(r"/announcements/article/(\d+)")
_GATE_TIME_RE = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+UTC")


def _fetch_listing_ids(session, base_url: str) -> List[str]:
    all_ids: List[str] = []
    seen: set = set()
    page = 1
    max_pages = 10
    while page <= max_pages:
        url = f"{base_url}?page={page}" if page > 1 else base_url
        response = session.get(url, headers=_GATE_HEADERS, timeout=20)
        LOGGER.info("Gate listing url=%s status=%s", url, response.status_code)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Gate listing response status=%s blocked_or_error", response.status_code)
            break
        response.raise_for_status()
        ids = _GATE_ARTICLE_ID_RE.findall(response.text)
        if not ids:
            break
        new_ids = [aid for aid in ids if aid not in seen]
        if not new_ids:
            break
        for aid in new_ids:
            seen.add(aid)
            all_ids.append(aid)
        page += 1
    LOGGER.info("Gate fetched %s unique article IDs across %s pages", len(all_ids), page)
    return all_ids


def _parse_gate_article(session, article_id: str, base_domain: str) -> Announcement | None:
    url = f"{base_domain}/announcements/article/{article_id}"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate article status=%s url=%s", response.status_code, url)
        return None
    response.raise_for_status()
    html = response.text
    time_match = _GATE_TIME_RE.search(html)
    timestamp = time_match.group(1) if time_match else None
    if not timestamp:
        return None
    published = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    soup = BeautifulSoup(html, "lxml")
    title = ""
    title_el = soup.find("h1")
    if title_el:
        title = title_el.get_text(strip=True)
    if not title:
        title = soup.title.get_text(strip=True) if soup.title else ""
    if not title:
        return None
    market_type = infer_market_type(title, default="spot")
    tickers = extract_tickers(title)
    return Announcement(
        source_exchange="Gate",
        title=title,
        published_at_utc=published,
        launch_at_utc=None,
        url=url,
        listing_type_guess=guess_listing_type(title),
        market_type=market_type,
        tickers=tickers,
        body="",
    )


def _fetch_from_domain(session, domain: str, cutoff: float) -> List[Announcement]:
    listings_url = f"{domain}/announcements/newlisted"
    ids = _fetch_listing_ids(session, listings_url)
    announcements: List[Announcement] = []
    for article_id in ids:
        announcement = _parse_gate_article(session, article_id, domain)
        if not announcement:
            continue
        if announcement.published_at_utc.timestamp() < cutoff:
            continue
        announcements.append(announcement)
    LOGGER.info("Gate parsed announcements=%s from %s", len(announcements), domain)
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    announcements = _fetch_from_domain(session, "https://www.gate.com", cutoff)
    if announcements:
        return announcements
    return _fetch_from_domain(session, "https://www.gate.tv", cutoff)
