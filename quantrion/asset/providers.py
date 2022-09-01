import asyncio
import re
from abc import ABC, abstractmethod
from time import time
from typing import Optional, Tuple

import pandas as pd
from pandas.core.window.rolling import Rolling

from ..asset.base import Asset
from ..settings import DEFAULT_TIMEFRAME as DTF
from ..utils import MarketDatetime as mdt


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

    def __init__(self, asset: Asset) -> None:
        self._asset = asset
        self._lock = asyncio.Lock()
        self._subscribed = False
        self._bars = None
        self._retrieved_range: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
        self._new_value_event = asyncio.Event()

    @property
    def asset(self) -> Asset:
        return self._asset

    @property
    def subscribed(self) -> bool:
        return self._subscribed

    @abstractmethod
    async def _retrieve(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        pass

    @abstractmethod
    async def _subscribe(self) -> None:
        pass

    async def subscribe(self) -> None:
        async with self._lock:
            if self._subscribed:
                return
            await self._subscribe()
            curr_ts = mdt.now().floor(DTF) - pd.Timedelta(DTF)
            if (
                self._retrieved_range is not None
                and (curr_end := self._retrieved_range[1]) < curr_ts
            ):
                await self._update_data(curr_end, curr_ts)
            self._subscribed = True

    def add(self, data: pd.DataFrame):
        if self._bars is None:
            self._bars = data
            self._retrieved_range = (data.index[0], data.index[-1])
        else:
            self._bars = pd.concat([self._bars, data])
            self._retrieved_range = (self._retrieved_range[0], data.index[-1])

    def notify(self) -> None:
        self._new_value_event.set()

    async def _rolling(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
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
            "min": 2 * n_retrieve_periods,
            "h": 2 * n_retrieve_periods,
            "d": 2
            * (
                n_retrieve_periods + 3 * (n_retrieve_periods // 7 + 1)
            ),  # +3 * weeks to account for weekends and public holidays
        }
        start = start - pd.Timedelta(timespan_to_delta[unit], unit=unit)
        data = await self.get(start, end, freq, True)
        return data[candle_key].rolling(n)

    async def get_sma(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 20,
        candle_key: str = "close",
    ) -> pd.Series:
        rolling = await self._rolling(start, end, freq, n, candle_key)
        return rolling.mean()[start:]

    async def get_bollinger_bands(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
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

    async def _update_data(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ):
        if self._retrieved_range is None:
            self._bars = await self._retrieve(start, end)
            self._retrieved_range = (start, end)
            return
        curr_start, curr_end = self._retrieved_range
        if start < curr_start:
            new_data = await self._retrieve(start, curr_start - pd.Timedelta(DTF))
            self._bars = pd.concat([new_data, self._bars])
            self._retrieved_range = (start, curr_end)
        if end > curr_end:
            new_data = await self._retrieve(curr_end + pd.Timedelta(DTF), end)
            self._bars = pd.concat([self._bars, new_data])
            self._retrieved_range = (self._retrieved_range[0], end)
        return

    async def _get(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        freq: Optional[str] = None,
        dropna: bool = True,
    ):
        if freq is None:
            return self._bars.loc[start:end]
        raw_data = self._bars.loc[start:end].copy()
        raw_data["price"] = raw_data["price"] * raw_data["volume"]
        data: pd.DataFrame = raw_data.resample(freq).aggregate(
            self._bars_resample_funcs
        )
        data["price"] /= data["volume"].where(data["volume"] != 0, 1e-9)
        if not dropna:
            return data.fillna(self._bars_fill_values)
        return data.dropna()

    async def get(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: Optional[str] = None,
        dropna: bool = True,
    ) -> pd.DataFrame:
        _freq = freq or DTF
        start = start.ceil(_freq)
        max_end = mdt.now().floor(_freq) - pd.Timedelta(DTF)
        if end is not None:
            end = end.floor(_freq)
            if _freq != DTF:
                end += pd.Timedelta(freq) - pd.Timedelta(DTF)
            end = min(end, max_end)
        else:
            end = max_end
        if start >= end:
            columns = set(self._bars_resample_funcs.keys())
            return pd.DataFrame(
                columns=columns, index=pd.DatetimeIndex([], tz=mdt.market_tz)
            )
        await self._update_data(start, end)
        data = await self._get(start, end, freq, dropna)
        return data

    async def wait_for_next(self, freq: Optional[str] = None) -> pd.Series:
        next_ts = mdt.now().ceil(freq or DTF).timestamp()
        while True:
            try:
                await asyncio.wait_for(
                    self._new_value_event.wait(),
                    timeout=next_ts - time() + 2,
                    # Wait up to 2 seconds after the current interval is over
                )
                if freq is None:
                    return self._bars.iloc[-1]
                if next_ts - time() > 0:
                    continue
            except asyncio.TimeoutError:
                next_ts = mdt.now().ceil(freq or DTF).timestamp()
                if freq is None:
                    continue
            finally:
                self._new_value_event.clear()
            start = mdt.now().floor(freq) - pd.Timedelta(freq)
            df = await self.get(start, freq=freq)
            if df.empty:
                continue
            return df.iloc[-1]
