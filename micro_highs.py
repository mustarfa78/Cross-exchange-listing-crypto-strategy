from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from mexc import Candle


@dataclass(frozen=True)
class MicroHighResult:
    max_price_1_close: Optional[float]
    max_price_1_time: Optional[datetime]
    lowest_after_1_close: Optional[float]
    lowest_after_1_time: Optional[datetime]
    max_price_2_close: Optional[float]
    max_price_2_time: Optional[datetime]
    lowest_after_2_close: Optional[float]
    lowest_after_2_time: Optional[datetime]
    notes: List[str]


@dataclass(frozen=True)
class MicroHighCandidate:
    bucket_start: datetime
    close3: float


@dataclass(frozen=True)
class ConfirmedMicroHigh:
    bucket_start: datetime
    close3: float
    micro_high_close: float
    micro_high_time: datetime
    pullback_low_close: Optional[float]
    pullback_low_time: Optional[datetime]
    pullback_size: Optional[float]
    pullback_pct: Optional[float]


def _floor_to_3m(ts: datetime) -> datetime:
    minute = ts.minute - (ts.minute % 3)
    return ts.replace(minute=minute, second=0, microsecond=0)


def _build_3m_series(candles: List[Candle]) -> Tuple[List[MicroHighCandidate], dict]:
    buckets = {}
    for candle in candles:
        bucket_start = _floor_to_3m(candle.timestamp)
        buckets.setdefault(bucket_start, []).append(candle)
    series = []
    for bucket_start in sorted(buckets.keys()):
        bucket_candles = sorted(buckets[bucket_start], key=lambda c: c.timestamp)
        close3 = bucket_candles[-1].close
        series.append(MicroHighCandidate(bucket_start=bucket_start, close3=close3))
    return series, buckets


def _confirmed_micro_highs(
    series: List[MicroHighCandidate],
    buckets: dict,
    candles: List[Candle],
    window_end: datetime,
    lookahead_bars: int,
    min_pullback_pct: float,
) -> List[ConfirmedMicroHigh]:
    confirmed: List[ConfirmedMicroHigh] = []
    for idx in range(1, len(series) - 1):
        current = series[idx]
        prev = series[idx - 1]
        nxt = series[idx + 1]
        if not (current.close3 > prev.close3 and current.close3 >= nxt.close3):
            continue
        lookahead = series[idx + 1 : idx + 1 + lookahead_bars]
        pullback_confirmed = False
        for future in lookahead:
            if future.close3 < current.close3 * (1 - min_pullback_pct):
                pullback_confirmed = True
                break
        if not pullback_confirmed:
            continue
        bucket_candles = buckets[current.bucket_start]
        micro_high_candle = max(bucket_candles, key=lambda c: c.close)
        confirmed.append(
            ConfirmedMicroHigh(
                bucket_start=current.bucket_start,
                close3=current.close3,
                micro_high_close=micro_high_candle.close,
                micro_high_time=micro_high_candle.timestamp,
                pullback_low_close=None,
                pullback_low_time=None,
                pullback_size=None,
                pullback_pct=None,
            )
        )
    if not confirmed:
        return []
    confirmed_with_pullback = []
    for idx, micro_high in enumerate(confirmed):
        next_boundary = window_end
        if idx + 1 < len(confirmed):
            next_boundary = confirmed[idx + 1].bucket_start
        search_start = micro_high.micro_high_time + timedelta(minutes=1)
        lows = [c for c in candles if search_start <= c.timestamp < next_boundary]
        if not lows:
            confirmed_with_pullback.append(micro_high)
            continue
        low_candle = min(lows, key=lambda c: c.close)
        pullback_size = micro_high.micro_high_close - low_candle.close
        pullback_pct = pullback_size / micro_high.micro_high_close if micro_high.micro_high_close else None
        confirmed_with_pullback.append(
            ConfirmedMicroHigh(
                bucket_start=micro_high.bucket_start,
                close3=micro_high.close3,
                micro_high_close=micro_high.micro_high_close,
                micro_high_time=micro_high.micro_high_time,
                pullback_low_close=low_candle.close,
                pullback_low_time=low_candle.timestamp,
                pullback_size=pullback_size,
                pullback_pct=pullback_pct,
            )
        )
    return confirmed_with_pullback


def compute_micro_highs(
    candles: List[Candle],
    window_start: datetime,
    window_end: datetime,
    lookahead_bars: int = 4,
    min_pullback_pct: float = 0.0,
) -> MicroHighResult:
    notes: List[str] = []
    filtered = [c for c in candles if window_start <= c.timestamp <= window_end]
    filtered.sort(key=lambda c: c.timestamp)
    if not filtered:
        notes.append("no candles in window")
        return MicroHighResult(None, None, None, None, None, None, None, None, notes)

    series, buckets = _build_3m_series(filtered)
    confirmed = _confirmed_micro_highs(
        series,
        buckets,
        filtered,
        window_end,
        lookahead_bars,
        min_pullback_pct,
    )

    max_price_2_candle = max(filtered, key=lambda c: c.close)
    max_price_2_close = max_price_2_candle.close
    max_price_2_time = max_price_2_candle.timestamp
    after_2 = [c for c in filtered if c.timestamp > max_price_2_time]
    lowest_after_2_close = None
    lowest_after_2_time = None
    if after_2:
        low_after_2 = min(after_2, key=lambda c: c.close)
        lowest_after_2_close = low_after_2.close
        lowest_after_2_time = low_after_2.timestamp
    else:
        notes.append("max price #2 at end of window")

    if not confirmed:
        notes.append("no confirmed micro highs")
        return MicroHighResult(
            None,
            None,
            None,
            None,
            max_price_2_close,
            max_price_2_time,
            lowest_after_2_close,
            lowest_after_2_time,
            notes,
        )

    max_pullback = max(
        confirmed,
        key=lambda c: c.pullback_size if c.pullback_size is not None else float("-inf"),
    )

    return MicroHighResult(
        max_pullback.micro_high_close,
        max_pullback.micro_high_time,
        max_pullback.pullback_low_close,
        max_pullback.pullback_low_time,
        max_price_2_close,
        max_price_2_time,
        lowest_after_2_close,
        lowest_after_2_time,
        notes,
    )


if __name__ == "__main__":
    import unittest

    class MicroHighTests(unittest.TestCase):
        def test_micro_highs_basic(self):
            start = datetime(2024, 1, 1, 0, 0)
            candles = [
                Candle(timestamp=start + timedelta(minutes=i), close=1 + i * 0.1)
                for i in range(15)
            ]
            result = compute_micro_highs(candles, start, start + timedelta(minutes=14))
            self.assertIsNotNone(result.max_price_2_close)

        def test_no_candles(self):
            start = datetime(2024, 1, 1, 0, 0)
            result = compute_micro_highs([], start, start + timedelta(minutes=60))
            self.assertIn("no candles in window", result.notes)

    unittest.main()
