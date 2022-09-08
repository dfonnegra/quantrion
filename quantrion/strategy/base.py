import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd

from ..asset.base import Asset
from ..data.base import AssetListProvider

logger = logging.getLogger(__name__)


class Strategy(ABC):
    def __init__(self, tl_provider: AssetListProvider):
        self._tl_provider = tl_provider
        self._tasks = []

    async def run(self):
        assets = await self._tl_provider.list_assets()
        self._tasks = [
            asyncio.create_task(self.run_for_asset(asset)) for asset in assets
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
    async def run_for_asset(self, asset: Asset):
        ...

    @abstractmethod
    async def evaluate_position(
        self, asset: Asset, last_bar: pd.Series
    ) -> PositionAction:
        ...
