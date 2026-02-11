from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

from http_client import get_json


LOGGER = logging.getLogger(__name__)


def resolve_market_cap(
    session,
    ticker: str,
    at_minus_1m: datetime,
    mexc_close_price: Optional[float],
) -> Tuple[Optional[float], str]:
    if not mexc_close_price:
        return None, "missing mexc close price for market cap"

    cmc_key = os.getenv("CMC_API_KEY")
    if cmc_key:
        try:
            cap = _cmc_market_cap(session, ticker, at_minus_1m, cmc_key)
            if cap:
                return cap, ""
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CMC market cap failed: %s", exc)
            return None, "CMC market cap failed"

    try:
        cap = _coingecko_market_cap(session, ticker, mexc_close_price)
        if cap:
            return cap, "market cap approximated via CoinGecko supply"
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("CoinGecko market cap failed: %s", exc)
        return None, "CoinGecko market cap failed"

    return None, "market cap unavailable"


def _cmc_market_cap(session, ticker: str, at_minus_1m: datetime, api_key: str) -> Optional[float]:
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/historical"
    headers = {"X-CMC_PRO_API_KEY": api_key}
    params = {
        "symbol": ticker.upper(),
        "time_start": int(at_minus_1m.replace(tzinfo=timezone.utc).timestamp()),
        "time_end": int(at_minus_1m.replace(tzinfo=timezone.utc).timestamp()),
        "count": 1,
        "interval": "5m",
    }
    response = session.get(url, params=params, headers=headers, timeout=20)
    response.raise_for_status()
    data = response.json()
    quotes = (
        data.get("data", {})
        .get(ticker.upper(), {})
        .get("quotes", [])
    )
    if not quotes:
        return None
    quote = quotes[0]
    return quote.get("quote", {}).get("USD", {}).get("market_cap")


def _coingecko_market_cap(session, ticker: str, mexc_close_price: float) -> Optional[float]:
    search_url = "https://api.coingecko.com/api/v3/search"
    search_data = get_json(session, search_url, params={"query": ticker})
    coins = search_data.get("coins", [])
    if not coins:
        return None
    coin_id = coins[0].get("id")
    if not coin_id:
        return None
    data = get_json(
        session,
        f"https://api.coingecko.com/api/v3/coins/{coin_id}",
        params={
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        },
    )
    market_data = data.get("market_data", {})
    supply = market_data.get("circulating_supply") or market_data.get("max_supply")
    if not supply:
        return None
    return float(supply) * float(mexc_close_price)
