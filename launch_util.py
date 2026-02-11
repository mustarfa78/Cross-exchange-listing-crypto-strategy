from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

LOGGER = logging.getLogger(__name__)


def _log_kline_attempt(
    exchange: str,
    symbol: str,
    window_start_ms: int,
    window_end_ms: int,
    candles: list[int],
    filtered_any: bool,
) -> None:
    if candles:
        LOGGER.debug(
            "launch_util: %s %s window %s-%s candles=%s min_ts=%s max_ts=%s filtered>=start=%s",
            exchange,
            symbol,
            window_start_ms,
            window_end_ms,
            len(candles),
            min(candles),
            max(candles),
            filtered_any,
        )
    else:
        LOGGER.debug(
            "launch_util: %s %s window %s-%s candles=0 filtered>=start=%s",
            exchange,
            symbol,
            window_start_ms,
            window_end_ms,
            filtered_any,
        )


def find_first_trade_time(
    fetch_klines_fn,
    start_ts_ms: int,
    interval_ms: int,
    max_lookahead_ms: int = 7 * 24 * 60 * 60 * 1000,
    limit: int = 1000,
    exchange: str = "",
    symbol: str = "",
) -> Optional[datetime]:
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    max_window_ms = min(16 * 60 * 60 * 1000, limit * interval_ms)
    window_ms = min(2 * 60 * 60 * 1000, max_window_ms)
    search_end_ms = start_ts_ms + max_lookahead_ms
    cursor_ms = start_ts_ms

    while cursor_ms < search_end_ms:
        window_end_ms = min(cursor_ms + window_ms, search_end_ms)
        candles = fetch_klines_fn(cursor_ms, window_end_ms, limit) or []
        candles = [int(ts) for ts in candles if ts is not None]
        filtered = [ts for ts in candles if ts >= start_ts_ms - interval_ms]

        if candles:
            max_ts = max(candles)
            if max_ts > window_end_ms + interval_ms * 2:
                LOGGER.debug(
                    "launch_util: %s %s endTime ignored (window_end=%s max_ts=%s now=%s)",
                    exchange,
                    symbol,
                    window_end_ms,
                    max_ts,
                    now_ms,
                )
                return None

        _log_kline_attempt(
            exchange,
            symbol,
            cursor_ms,
            window_end_ms,
            candles,
            bool(filtered),
        )

        if filtered:
            first_ts = min(filtered)
            LOGGER.info("launch_util: LAUNCH_FOUND %s %s %s", exchange, symbol, first_ts)
            return datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)

        if not candles:
            cursor_ms = window_end_ms
        else:
            cursor_ms = window_end_ms
        if window_ms < max_window_ms:
            window_ms = min(window_ms * 2, max_window_ms)

    LOGGER.info("launch_util: LAUNCH_NOT_FOUND %s %s", exchange, symbol)
    return None


def _fetch_first_candle_binance(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            "symbol": f"{ticker}USDT",
            "interval": "1m",
            "startTime": start_ts * 1000,
            "limit": 1
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Binance Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": f"{ticker}USDT",
            "interval": "1m",
            "startTime": start_ts * 1000,
            "limit": 1
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Binance Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_bybit(session, ticker: str, start_ts: int) -> Optional[datetime]:
    def _fetch_bybit_klines(category: str):
        def _fetch(start_ms: int, end_ms: int, limit: int) -> list[int]:
            url = "https://api.bybit.com/v5/market/kline"
            params = {
                "category": category,
                "symbol": f"{ticker}USDT",
                "interval": "1",
                "start": start_ms,
                "end": end_ms,
                "limit": limit,
            }
            resp = session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return []
            data = resp.json()
            candles = data.get("result", {}).get("list") if data.get("retCode") == 0 else []
            return [int(item[0]) for item in candles] if candles else []

        return _fetch

    start_ts_ms = start_ts * 1000
    interval_ms = 60 * 1000
    # Try Futures (Linear) first
    try:
        return find_first_trade_time(
            _fetch_bybit_klines("linear"),
            start_ts_ms,
            interval_ms,
            exchange="Bybit",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("Bybit Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        return find_first_trade_time(
            _fetch_bybit_klines("spot"),
            start_ts_ms,
            interval_ms,
            exchange="Bybit",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("Bybit Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_gate(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
        params = {
            "contract": f"{ticker}_USDT",
            "interval": "1m",
            "limit": 1,
            "from": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                item = data[0]
                ts = item.get("t")
                if ts:
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Gate Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.gateio.ws/api/v4/spot/candlesticks"
        params = {
            "currency_pair": f"{ticker}_USDT",
            "interval": "1m",
            "limit": 1,
            "from": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Gate Spot: [time, volume, close, high, low, open]
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Gate Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_bitget(session, ticker: str, start_ts: int) -> Optional[datetime]:
    def _fetch_bitget_klines(endpoint: str, params: dict):
        def _fetch(start_ms: int, end_ms: int, limit: int) -> list[int]:
            payload = dict(params)
            payload.update({"startTime": start_ms, "endTime": end_ms, "limit": limit})
            resp = session.get(endpoint, params=payload, timeout=10)
            if resp.status_code != 200:
                return []
            data = resp.json()
            if data.get("code") != "00000":
                return []
            candles = data.get("data") or []
            return [int(item[0]) for item in candles] if candles else []

        return _fetch

    start_ts_ms = start_ts * 1000
    interval_ms = 60 * 1000

    # Try Futures (Mix) first
    try:
        return find_first_trade_time(
            _fetch_bitget_klines(
                "https://api.bitget.com/api/v2/mix/market/candles",
                {
                    "symbol": f"{ticker}USDT",
                    "granularity": "1m",
                    "productType": "USDT-FUTURES",
                },
            ),
            start_ts_ms,
            interval_ms,
            exchange="Bitget",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("Bitget Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        return find_first_trade_time(
            _fetch_bitget_klines(
                "https://api.bitget.com/api/v2/spot/market/candles",
                {"symbol": f"{ticker}USDT", "granularity": "1min"},
            ),
            start_ts_ms,
            interval_ms,
            exchange="Bitget",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("Bitget Spot check failed for %s: %s", ticker, e)

    return None

def _fetch_first_candle_xt(session, ticker: str, start_ts: int) -> Optional[datetime]:
    def _fetch_xt_futures(start_ms: int, end_ms: int, limit: int) -> list[int]:
        url = "https://fapi.xt.com/future/market/v1/public/q/kline"
        params = {
            "symbol": f"{ticker.lower()}_usdt",
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        res = data.get("result") or data.get("data")
        if not isinstance(res, list):
            return []
        return [int(item.get("t")) for item in res if item.get("t") is not None]

    def _fetch_xt_spot(start_ms: int, end_ms: int, limit: int) -> list[int]:
        url = "https://sapi.xt.com/v4/public/kline"
        params = {
            "symbol": f"{ticker.lower()}_usdt",
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        res = data.get("result") or data.get("data")
        if not isinstance(res, list):
            return []
        return [int(item.get("t")) for item in res if item.get("t") is not None]

    start_ts_ms = start_ts * 1000
    interval_ms = 60 * 1000

    # Try Futures first
    try:
        return find_first_trade_time(
            _fetch_xt_futures,
            start_ts_ms,
            interval_ms,
            exchange="XT",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("XT Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        return find_first_trade_time(
            _fetch_xt_spot,
            start_ts_ms,
            interval_ms,
            exchange="XT",
            symbol=ticker,
        )
    except Exception as e:
        LOGGER.debug("XT Spot check failed for %s: %s", ticker, e)

    return None

def _fetch_first_candle_kucoin(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://api-futures.kucoin.com/api/v1/kline/query"
        params = {
            "symbol": f"{ticker}USDTM",
            "granularity": 1,
            "from": start_ts * 1000,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "200000" and data.get("data"):
                candles = data["data"]
                if candles:
                    # KuCoin Futures assumed ascending (oldest first). Use candles[0].
                    ts = int(candles[0][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("KuCoin Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.kucoin.com/api/v1/market/candles"
        params = {
            "symbol": f"{ticker}-USDT",
            "type": "1min",
            "startAt": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "200000" and data.get("data"):
                candles = data["data"]
                if candles:
                    # KuCoin Spot returns descending (newest first). Use candles[-1].
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("KuCoin Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_kraken(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Spot only
    try:
        url = "https://api.kraken.com/0/public/OHLC"
        params = {"pair": f"{ticker}USD", "since": start_ts}
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if not data.get("error") and data.get("result"):
                res = data["result"]
                for key, val in res.items():
                    if key == "last":
                        continue
                    if isinstance(val, list) and val:
                        ts = int(val[0][0])
                        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Kraken Spot check failed for %s: %s", ticker, e)

    return None


def resolve_launch_time(
    session,
    source_exchange: str,
    ticker: str,
    search_start_time: Optional[datetime] = None
) -> Optional[datetime]:
    """
    Resolves the launch time (start of trading) for a given ticker.

    Args:
        session: Requests session
        source_exchange: Name of the exchange
        ticker: Ticker symbol (e.g. BTC)
        search_start_time: Optional datetime to start searching from (e.g. announcement time).
                           If not provided, defaults to Jan 1 2020.
    """
    # Default to 2020 if no time provided
    start_dt = search_start_time if search_start_time else datetime(2020, 1, 1, tzinfo=timezone.utc)
    # Ensure UTC
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    start_ts = int(start_dt.timestamp())

    launch_time = None
    try:
        if source_exchange == "Binance":
            launch_time = _fetch_first_candle_binance(session, ticker, start_ts)
        elif source_exchange == "Bybit":
            launch_time = _fetch_first_candle_bybit(session, ticker, start_ts)
        elif source_exchange == "Gate":
            launch_time = _fetch_first_candle_gate(session, ticker, start_ts)
        elif source_exchange == "Bitget":
            launch_time = _fetch_first_candle_bitget(session, ticker, start_ts)
        elif source_exchange == "KuCoin":
            launch_time = _fetch_first_candle_kucoin(session, ticker, start_ts)
        elif source_exchange == "XT":
            launch_time = _fetch_first_candle_xt(session, ticker, start_ts)
        elif source_exchange == "Kraken":
            launch_time = _fetch_first_candle_kraken(session, ticker, start_ts)

        if launch_time:
            LOGGER.info(
                "launch_util: Resolved launch time for %s on %s: %s",
                ticker,
                source_exchange,
                launch_time.isoformat(),
            )
        else:
            LOGGER.warning(
                "launch_util: Could not resolve launch time for %s on %s (searched from %s)",
                ticker,
                source_exchange,
                start_dt.isoformat(),
            )

        return launch_time

    except Exception as exc:
        LOGGER.error(
            "launch_util: Error resolving launch time for %s on %s: %s",
            ticker,
            source_exchange,
            exc,
            exc_info=True,
        )
        return None
