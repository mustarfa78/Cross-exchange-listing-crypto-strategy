from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from http_client import get_json


LOGGER = logging.getLogger(__name__)
BASE_URL = "https://contract.mexc.com"
CONTRACT_DETAIL_URL = f"{BASE_URL}/api/v1/contract/detail"


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    close: float


@dataclass(frozen=True)
class ContractInfo:
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str


class MexcFuturesClient:
    def __init__(self, session):
        self.session = session
        self._use_ms: Optional[bool] = None

    def list_contracts(self) -> List[ContractInfo]:
        data = get_json(self.session, CONTRACT_DETAIL_URL)
        items = data.get("data") or []
        if isinstance(items, dict):
            items = items.get("list") or items.get("items") or []
        if not isinstance(items, list):
            items = []
        contracts = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or item.get("contractCode") or "").upper()
            base_asset = str(item.get("baseCoin") or item.get("base") or "").upper()
            quote_asset = str(item.get("quoteCoin") or item.get("quote") or "").upper()
            contract_type = str(item.get("contractType") or item.get("type") or "").lower()
            status = item.get("status")
            if isinstance(status, str) and status.lower() in ("offline", "disabled", "suspend"):
                continue
            if not symbol or not base_asset:
                continue
            contracts.append(
                ContractInfo(
                    symbol=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    contract_type=contract_type,
                )
            )
        return contracts

    def map_ticker_to_symbols(self, ticker: str, contracts: Iterable[ContractInfo]) -> List[str]:
        matches = [
            contract
            for contract in contracts
            if contract.base_asset.upper() == ticker.upper()
        ]
        if not matches:
            return []
        def _priority(contract: ContractInfo) -> Tuple[int, str]:
            is_usdt = 0 if "USDT" in contract.symbol or contract.quote_asset == "USDT" else 1
            is_perp = 0 if "perp" in contract.contract_type else 1
            return (is_usdt, is_perp, contract.symbol)
        matches.sort(key=_priority)
        return [contract.symbol for contract in matches]

    def fetch_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "Min1",
        limit: Optional[int] = None,
    ) -> List[Candle]:
        start_time = start_time.replace(second=0, microsecond=0, tzinfo=timezone.utc)
        end_time = end_time.replace(second=0, microsecond=0, tzinfo=timezone.utc)
        url = f"{BASE_URL}/api/v1/contract/kline/{symbol}"
        for use_ms in self._time_units_to_try():
            start_ts = int(start_time.timestamp() * (1000 if use_ms else 1))
            end_ts = int(end_time.timestamp() * (1000 if use_ms else 1))
            params = {"interval": interval, "start": start_ts, "end": end_ts}
            if limit:
                params["limit"] = limit
            LOGGER.info(
                "MEXC kline request url=%s params=%s start=%s end=%s use_ms=%s",
                url,
                params,
                start_ts,
                end_ts,
                use_ms,
            )
            response = self.session.get(url, params=params, timeout=20)
            body_preview = response.text[:300] if response.text else ""
            LOGGER.info(
                "MEXC kline response status=%s body_preview=%s",
                response.status_code,
                body_preview,
            )
            response.raise_for_status()
            data = response.json()
            candles = self._parse_kline_payload(data)
            if candles:
                self._use_ms = use_ms
                return candles
            LOGGER.info("MEXC kline empty keys=%s use_ms=%s", list(data.keys()), use_ms)
        return []

    def _time_units_to_try(self) -> List[bool]:
        if self._use_ms is None:
            return [False, True]
        return [self._use_ms]

    def _parse_kline_payload(self, data: dict) -> List[Candle]:
        candles: List[Candle] = []
        payload = data.get("data", [])
        if isinstance(payload, dict):
            times = payload.get("time") or []
            closes = payload.get("close") or []
            for idx, ts in enumerate(times):
                try:
                    close = float(closes[idx])
                    ts_val = int(ts)
                    if ts_val < 10_000_000_000:
                        ts_val *= 1000
                    candle_time = datetime.fromtimestamp(ts_val / 1000, tz=timezone.utc)
                except (IndexError, ValueError, TypeError):
                    continue
                candles.append(Candle(timestamp=candle_time, close=close))
            candles.sort(key=lambda c: c.timestamp)
            return candles
        if isinstance(payload, list):
            for item in payload:
                try:
                    ts = int(item[0])
                    if ts < 10_000_000_000:
                        ts *= 1000
                    candle_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                    close = float(item[4])
                except (IndexError, ValueError, TypeError):
                    continue
                candles.append(Candle(timestamp=candle_time, close=close))
            candles.sort(key=lambda c: c.timestamp)
        return candles

    def has_candle_covering(self, candles: List[Candle], target: datetime) -> bool:
        target = target.replace(second=0, microsecond=0, tzinfo=timezone.utc)
        for candle in candles:
            if candle.timestamp.replace(second=0, microsecond=0) == target:
                return True
        return False

    def get_close_at(self, candles: List[Candle], target: datetime) -> Optional[float]:
        for candle in candles:
            if candle.timestamp == target:
                return candle.close
        return None

    def ensure_trading(self, symbol: str, at_time: datetime) -> Tuple[bool, List[Candle]]:
        at_time = at_time.astimezone(timezone.utc)
        target = at_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
        start_time = at_time - timedelta(minutes=10)
        end_time = at_time + timedelta(minutes=2)
        candles = self.fetch_klines(symbol, start_time=start_time, end_time=end_time)
        exists = self.has_candle_covering(candles, target)
        if not exists:
            LOGGER.info("No candle for %s at %s", symbol, target.isoformat())
        return exists, candles

    def check_symbol_live_now(self, symbol: str) -> bool:
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=30)
        end_time = now
        candles = self.fetch_klines(symbol, start_time=start_time, end_time=end_time)
        return bool(candles)

    def probe_first_contracts(self, contracts: List[ContractInfo]) -> None:
        for contract in contracts[:2]:
            try:
                candles = self.fetch_klines(
                    contract.symbol,
                    start_time=datetime.now(timezone.utc) - timedelta(minutes=30),
                    end_time=datetime.now(timezone.utc),
                )
                LOGGER.info(
                    "Probe contract %s candles=%s",
                    contract.symbol,
                    len(candles),
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Probe failed for %s: %s", contract.symbol, exc)
