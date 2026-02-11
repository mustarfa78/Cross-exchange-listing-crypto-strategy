from adapters.binance import fetch_announcements as fetch_binance
from adapters.bitget import fetch_announcements as fetch_bitget
from adapters.bybit import fetch_announcements as fetch_bybit
from adapters.gate import fetch_announcements as fetch_gate
from adapters.kraken import fetch_announcements as fetch_kraken
from adapters.kucoin import fetch_announcements as fetch_kucoin
from adapters.xt import fetch_announcements as fetch_xt

__all__ = [
    "fetch_binance",
    "fetch_bitget",
    "fetch_bybit",
    "fetch_gate",
    "fetch_kraken",
    "fetch_kucoin",
    "fetch_xt",
]
