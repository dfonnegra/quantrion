import asyncio
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd

from .. import settings
from ..utils import MarketDatetime as mdt


class BarsFixture(ABC):
    _bars_resample_funcs = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "price": "sum",
    }
    _bars_fill_values = {
        "volume": 0,
    }
    _lock: asyncio.Lock = None
    _subscribed_bars: bool = False
    _bars: pd.DataFrame = None

    @abstractmethod
    async def _retrieve_bars(
        self, start: datetime, end: Optional[datetime] = None
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    async def _subscribe_bars(self) -> None:
        pass

    async def subscribe_bars(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._subscribed_bars:
                return
            await self._subscribe_bars()
            curr_ts = pd.Timestamp(mdt.now()).floor(settings.DEFAULT_TIMEFRAME)
            if self._bars is not None and (last_ts := self._bars.index[-1]) < curr_ts:
                await self.get_bars(last_ts)
            self._subscribed_bars = True

    def add_bars(self, data: pd.DataFrame):
        if self._bars is None:
            self._bars = data
            return
        self._bars = pd.concat([self._bars, data])

    async def _update_bars(self, start: datetime, end: Optional[datetime] = None):
        if self._bars is None:
            self._bars = await self._retrieve_bars(start, end)
            return
        if start < self._bars.index[0]:
            data = await self._retrieve_bars(
                start, self._bars.index[0] - pd.Timedelta(settings.DEFAULT_TIMEFRAME)
            )
            self._bars = pd.concat([data, self._bars])
        curr_end = self._bars.index[-1]
        if not self._subscribed_bars and (end is None or end > curr_end):
            data = await self._retrieve_bars(
                self._bars.index[-1] + pd.Timedelta(settings.DEFAULT_TIMEFRAME), end
            )
            self._bars = pd.concat([self._bars, data])

    async def get_bars(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        freq: str = None,
    ) -> pd.DataFrame:
        await self._update_bars(start, end)
        if end is not None:
            raw_data = self._bars.loc[start:end].copy()
        else:
            raw_data = self._bars.loc[start:].copy()
        if freq is None:
            return raw_data
        raw_data["price"] = raw_data["price"] * raw_data["volume"]
        data: pd.DataFrame = raw_data.resample(freq).aggregate(
            self._bars_resample_funcs
        )
        data["price"] /= data["volume"].where(data["volume"] != 0, 1e-9)
        data = data.fillna(self._bars_fill_values)
        na_index = data["close"].isna()
        data["close"] = data["close"].fillna(method="ffill")
        for col in ["open", "high", "low"]:
            data.loc[na_index, col] = data["close"]
        return data.fillna(method="ffill")
