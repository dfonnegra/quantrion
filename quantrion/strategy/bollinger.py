from typing import Protocol

from ..ticker.providers import BarsProvider
from .base import Strategy


class BollingerTicker(Protocol):
    bars: BarsProvider


class SimpleBollingerStrategy(Strategy):
    async def run_for_ticker(self, ticker: BollingerTicker):
        ticker.bars
