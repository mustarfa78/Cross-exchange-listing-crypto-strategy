from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from mexc import Candle


@dataclass(frozen=True)
class LaunchHighLowResult:
    highest_close: Optional[float]
    highest_time: Optional[datetime]
    pullback_1_close: Optional[float]
    pullback_1_time: Optional[datetime]
    lowest_close: Optional[float]
    lowest_time: Optional[datetime]
    pullback_2_close: Optional[float]
    pullback_2_time: Optional[datetime]


@dataclass(frozen=True)
class CandleBucket:
    start_time: datetime
    candles: List[Candle]
    close: float  # Close of the last candle in bucket (or any representative close)


def _floor_to_3m(ts: datetime) -> datetime:
    minute = ts.minute - (ts.minute % 3)
    return ts.replace(minute=minute, second=0, microsecond=0)


def _build_3m_series(candles: List[Candle]) -> List[CandleBucket]:
    buckets = {}
    for candle in candles:
        bucket_start = _floor_to_3m(candle.timestamp)
        buckets.setdefault(bucket_start, []).append(candle)

    series = []
    for bucket_start in sorted(buckets.keys()):
        bucket_candles = sorted(buckets[bucket_start], key=lambda c: c.timestamp)
        # Using the close of the last candle in the bucket as the bucket's close,
        # consistent with typical OHLC resampling, although the requirement says
        # "Find the 3-minute bucket with the lowest close".
        # Assuming this means the close price of the 3m bar.
        close_val = bucket_candles[-1].close
        series.append(CandleBucket(start_time=bucket_start, candles=bucket_candles, close=close_val))
    return series


def compute_launch_highlow(candles: List[Candle], launch_time: datetime) -> LaunchHighLowResult:
    window_end = launch_time + timedelta(minutes=120)

    # Filter candles to range (launch_time, launch_time + 120 minutes]
    # Strictly AFTER launch_time
    window_candles = [
        c for c in candles
        if launch_time < c.timestamp <= window_end
    ]
    window_candles.sort(key=lambda c: c.timestamp)

    if not window_candles:
        return LaunchHighLowResult(None, None, None, None, None, None, None, None)

    # 1. Highest Close
    high_candle = max(window_candles, key=lambda c: c.close)
    highest_close = high_candle.close
    highest_time = high_candle.timestamp

    # 2. Pullback #1 (Dip after High)
    pullback_1_close = None
    pullback_1_time = None

    after_high_candles = [c for c in window_candles if c.timestamp > highest_time]
    if after_high_candles:
        buckets_after_high = _build_3m_series(after_high_candles)
        if buckets_after_high:
            # Find the 3-minute bucket with the lowest close
            lowest_bucket = min(buckets_after_high, key=lambda b: b.close)

            # Inside that winning 3-minute bucket, find the specific 1-minute candle with the lowest close
            pullback_1_candle = min(lowest_bucket.candles, key=lambda c: c.close)
            pullback_1_close = pullback_1_candle.close
            pullback_1_time = pullback_1_candle.timestamp

    # 3. Lowest Close
    low_candle = min(window_candles, key=lambda c: c.close)
    lowest_close = low_candle.close
    lowest_time = low_candle.timestamp

    # 4. Pullback #2 (Bounce after Low)
    pullback_2_close = None
    pullback_2_time = None

    after_low_candles = [c for c in window_candles if c.timestamp > lowest_time]
    if after_low_candles:
        buckets_after_low = _build_3m_series(after_low_candles)
        if buckets_after_low:
            # Find the 3-minute bucket with the highest close
            highest_bucket = max(buckets_after_low, key=lambda b: b.close)

            # Inside that bucket, find the specific 1-minute candle with the highest close
            pullback_2_candle = max(highest_bucket.candles, key=lambda c: c.close)
            pullback_2_close = pullback_2_candle.close
            pullback_2_time = pullback_2_candle.timestamp

    return LaunchHighLowResult(
        highest_close=highest_close,
        highest_time=highest_time,
        pullback_1_close=pullback_1_close,
        pullback_1_time=pullback_1_time,
        lowest_close=lowest_close,
        lowest_time=lowest_time,
        pullback_2_close=pullback_2_close,
        pullback_2_time=pullback_2_time,
    )


if __name__ == "__main__":
    import unittest

    class TestLaunchHighLow(unittest.TestCase):
        def test_basic_high_low_pullback(self):
            base_time = datetime(2024, 1, 1, 10, 0)
            candles = []

            # Up to High at 10:10
            # Note: base_time is launch time. c.timestamp > base_time.
            # So 10:00 is excluded.
            for i in range(11):
                t = base_time + timedelta(minutes=i)
                price = 60 + i * 4 # 60, 64, ... 100
                candles.append(Candle(t, float(price)))

            # Dip after high (Pullback 1)
            # 10:11 to 10:15 down
            for i in range(1, 6):
                t = base_time + timedelta(minutes=10 + i)
                price = 100 - i * 2 # 98, 96, 94, 92, 90
                candles.append(Candle(t, float(price)))

            # Continue down to Low at 10:40
            for i in range(1, 26): # 25 mins
                t = base_time + timedelta(minutes=15 + i) # starts 10:16
                price = 90 - i * 1.6 # down to ~50
                candles.append(Candle(t, float(price)))

            # Bounce after low (Pullback 2)
            # 10:41 to 10:50 up
            for i in range(1, 11):
                t = base_time + timedelta(minutes=40 + i)
                price = 50 + i * 2 # up to 70
                candles.append(Candle(t, float(price)))

            # Verify inputs roughly

            res = compute_launch_highlow(candles, base_time)

            # Note: 10:00 (60.0) is filtered out because > launch_time.
            # Max is still 100 at 10:10.
            self.assertEqual(res.highest_close, 100.0)
            self.assertEqual(res.highest_time, base_time + timedelta(minutes=10))

            self.assertAlmostEqual(res.lowest_close, 50.0, delta=0.1)
            # Low is at 10:40 (15+25)
            self.assertEqual(res.lowest_time, base_time + timedelta(minutes=40))

            self.assertIsNotNone(res.pullback_1_close)
            self.assertLess(res.pullback_1_close, 100.0)

            self.assertIsNotNone(res.pullback_2_close)
            self.assertGreater(res.pullback_2_close, 50.0)

        def test_120m_window_inclusion(self):
            base_time = datetime(2024, 1, 1, 10, 0)
            candles = []
            # Candle at 10:00 (launch) -> Should be excluded
            candles.append(Candle(base_time, 10.0))
            # Candle at 11:59 (119m) -> Should be included
            candles.append(Candle(base_time + timedelta(minutes=119), 20.0))
            # Candle at 12:00 (120m) -> Should be included
            candles.append(Candle(base_time + timedelta(minutes=120), 30.0))
            # Candle at 12:01 (121m) -> Should be excluded
            candles.append(Candle(base_time + timedelta(minutes=121), 40.0))

            res = compute_launch_highlow(candles, base_time)

            self.assertEqual(res.highest_close, 30.0)
            self.assertEqual(res.highest_time, base_time + timedelta(minutes=120))
            self.assertEqual(res.lowest_close, 20.0)

        def test_empty_window(self):
            base_time = datetime(2024, 1, 1, 10, 0)
            res = compute_launch_highlow([], base_time)
            self.assertIsNone(res.highest_close)
            self.assertIsNone(res.lowest_close)

    unittest.main()
