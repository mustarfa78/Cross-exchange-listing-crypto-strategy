from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential


LOGGER = logging.getLogger(__name__)


def build_session(cache_name: str = "http_cache", expire_seconds: int = 10800) -> requests.Session:
    session = requests_cache.CachedSession(
        cache_name=cache_name,
        backend="sqlite",
        expire_after=expire_seconds,
    )
    session.headers.update(
        {
            "User-Agent": "mexc-futures-listing-analyzer/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return session


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def get_json(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    LOGGER.debug("GET %s params=%s", url, params)
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def get_text(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> str:
    LOGGER.debug("GET %s params=%s", url, params)
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.text
