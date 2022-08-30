import asyncio
import logging
from abc import ABC, abstractmethod

from ..ticker.ticker import Ticker, TickerListProvider

logger = logging.getLogger(__name__)


class Strategy(ABC):
    def __init__(self, tl_provider: TickerListProvider):
        self._tl_provider = tl_provider
        self._tasks = []

    async def run(self):
        tickers = await self._tl_provider.list_tickers()
        self._tasks = [
            asyncio.create_task(self.run_for_ticker(ticker)) for ticker in tickers
        ]
        await asyncio.gather(*self._tasks)

    async def stop(self):
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @abstractmethod
    async def run_for_ticker(self, ticker: Ticker):
        pass
