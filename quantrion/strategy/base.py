import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd

from ..asset.base import Asset, TradableAsset
from ..data.base import AssetListProvider

logger = logging.getLogger(__name__)


class Strategy(ABC):
    def __init__(self, tl_provider: AssetListProvider, freq: str = None):
        self._tl_provider = tl_provider
        self._freq = freq
        self._tasks = []

    async def run(self):
        assets = await self._tl_provider.list_assets()
        self._tasks = [
            asyncio.create_task(self.run_for_asset(asset)) for asset in assets
        ]
        return await asyncio.gather(*self._tasks)

    async def stop(self):
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def run_for_asset(self, asset: TradableAsset):
        while True:
            last_bar = await asset.bars.wait_for_next(self._freq)
            await self.next(asset, last_bar)

    @abstractmethod
    async def next(self, asset: Asset, last_bar: pd.Series):
        ...
