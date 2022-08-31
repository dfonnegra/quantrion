from typing import Optional, Protocol

from ..ticker.providers import BarsProvider
from .base import Strategy


class BollingerTicker(Protocol):
    bars: BarsProvider


class SimpleBollingerStrategy(Strategy):
    def __init__(
        self,
        freq: Optional[str] = None,
        short_n: int = 20,
        long_n: int = 40,
    ) -> None:
        self._freq = freq
        self._short_n = short_n
        self._long_n = long_n

    async def run_for_ticker(self, ticker: BollingerTicker):
        while True:
            last_bar = await ticker.bars.wait_for_next(self._freq)
            long_sma = await ticker.bars.get_sma(
                last_bar.index[0], freq=self._freq, n=self._long_n
            )
            lower, sma, upper = await ticker.bars.get_bollinger_bands(
                last_bar.index[0], freq=self._freq
            )
            if lower[-1] < last_bar["close"][-1] < upper[-1]:
                pass
