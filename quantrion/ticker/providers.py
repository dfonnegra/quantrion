import asyncio
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Coroutine, Optional, Protocol, Tuple

import pandas as pd
from pandas.core.window.rolling import Rolling

from .. import settings
from ..utils import MarketDatetime as mdt


async def _update_data(
    start: datetime,
    end: datetime,
    current_data: Optional[pd.DataFrame],
    retriever: Callable[[datetime, datetime], Coroutine[Any, Any, pd.DataFrame]],
):
    data = current_data
    if data is None or data.empty:
        return await retriever(start, end)
    if start < data.index[0]:
        new_data = await retriever(start, data.index[0])
        if data.index[0] in new_data.index:
            data = data.iloc[1:]
        data = pd.concat([new_data, data])
    if end > data.index[-1]:
        new_data = await retriever(data.index[-1], end)
        if data.index[-1] in new_data.index:
            data = data.iloc[:-1]
        data = pd.concat([data, new_data])
    return data


class BarsProvider(ABC):
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

    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._lock = asyncio.Lock()
        self._subscribed = False
        self._bars = None

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def subscribed(self) -> bool:
        return self._subscribed

    @abstractmethod
    async def _retrieve(self, start: datetime, end: datetime) -> pd.DataFrame:
        pass

    @abstractmethod
    async def _subscribe(self) -> None:
        pass

    async def subscribe(self) -> None:
        async with self._lock:
            if self._subscribed:
                return
            await self._subscribe()
            curr_ts = pd.Timestamp(mdt.now()).floor(settings.DEFAULT_TIMEFRAME)
            if self._bars is not None and (last_ts := self._bars.index[-1]) < curr_ts:
                await self.get(last_ts)
            self._subscribed = True

    def add(self, data: pd.DataFrame):
        if self._bars is None:
            self._bars = data
            return
        self._bars = pd.concat([self._bars, data])

    async def _rolling(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        freq: str = None,
        n: int = 20,
        candle_key: str = "close",
    ) -> Rolling:
        """
        Returns a rolling object for the given parameters.
        Note:
            The returned rolling object might have extra rows at the beginning (before start)
            so, make sure to filter after executing the operation.
        """
        periods, unit = re.search(r"(\d+)(min|h|d)", freq, flags=re.IGNORECASE).groups()
        periods, unit = int(periods), unit.lower()
        n_retrieve_periods = n * periods
        timespan_to_delta = {
            # We multiply the number of periods by 2 to count on missing data
            "min": timedelta(minutes=2 * n_retrieve_periods),
            "h": timedelta(hours=2 * n_retrieve_periods),
            "d": timedelta(
                days=2 * (n_retrieve_periods + 3 * (n_retrieve_periods // 7 + 1))
            ),  # +3 * weeks to account for weekends and public holidays
        }
        start = start - timespan_to_delta[unit]
        data = await self.get(start, end, freq)
        return data[candle_key].rolling(n)

    async def get_sma(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        freq: str = None,
        n: int = 20,
        candle_key: str = "close",
    ) -> pd.Series:
        rolling = await self._rolling(start, end, freq, n, candle_key)
        return rolling.mean()[start:]

    async def get_bollinger_bands(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        freq: str = None,
        n: int = 20,
        k: float = 2,
        candle_key: str = "close",
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        rolling = await self._rolling(start, end, freq, n, candle_key)
        sma = rolling.mean()[start:]
        std = rolling.std()[start:]
        upper = sma + k * std
        lower = sma - k * std
        return lower, sma, upper

    async def get(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        freq: str = None,
    ) -> pd.DataFrame:
        start = pd.Timestamp(start).ceil(settings.DEFAULT_TIMEFRAME)
        max_end = mdt.now() - pd.Timedelta(settings.DEFAULT_TIMEFRAME)
        end = max_end if end is None else min(end, max_end)
        end = pd.Timestamp(end).floor(settings.DEFAULT_TIMEFRAME)
        self._bars = await _update_data(start, end, self._bars, self._retrieve)
        raw_data = self._bars.loc[start:end].copy()
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
