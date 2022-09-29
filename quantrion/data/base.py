import asyncio
import math
import re
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, List, Optional, Tuple

import pandas as pd

from ..asset.base import Asset
from ..settings import DEFAULT_TIMEFRAME as DTF


class AssetListProvider(ABC):
    @abstractmethod
    async def list_assets(self) -> List[Asset]:
        pass


class GenericBarsProvider(ABC):
    _bars_resample_funcs = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "price": "sum",
    }

    @abstractmethod
    async def _retrieve(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Retrieves the bars data in the interval [start, end]
        """

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

    def add(self, data: pd.DataFrame):
        self._new_value_event.set()
        if self._bars is None:
            self._bars = data
            self._retrieved_range = (data.index[0], data.index[-1])
        else:
            self._bars = pd.concat([self._bars, data])
            self._retrieved_range = (self._retrieved_range[0], data.index[-1])

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

    def _get_required_start_end(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 0,
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """
        Returns the normalized start and end timestamps.
        1. A normalized start is the ce start timestamp that ensures having n periods of data before the start timestamp and that lies on a freq.
        2. A normalized end is the closest end timestamp that lies on a freq. If no end is given, the max timestamp is returned.
        3. The max timestamp is the floor of the current timestamp minus the freq.

        """
        _freq = freq or DTF
        start = start.ceil(_freq)
        now = self.asset.localize(pd.Timestamp.utcnow())
        max_end = now.floor(_freq) - pd.Timedelta(DTF)
        if end is not None:
            end = end.floor(_freq)
            if _freq != DTF:
                end += pd.Timedelta(freq) - pd.Timedelta(DTF)
            end = min(end, max_end)
        else:
            end = max_end
        if n == 0:
            return start, end
        periods, unit = re.search(
            r"(\d+)(min|h|d)", _freq, flags=re.IGNORECASE
        ).groups()
        periods, unit = int(periods), unit.lower()
        n_retrieve_periods = n * periods
        if len(_bars_before := self._bars.loc[:start]) >= n_retrieve_periods + 1:
            return _bars_before.index[-n_retrieve_periods - 1], end
        n_weeks = math.ceil(n_retrieve_periods / 7)
        timespan_to_delta = {
            # We multiply the number of periods by 2 to count on missing data
            "min": 2 * n_retrieve_periods,
            "h": 2 * n_retrieve_periods,
            "d": 2
            * (
                n_retrieve_periods + 3 * n_weeks
            ),  # +3 * weeks to account for weekends and public holidays
        }
        start = start - pd.Timedelta(timespan_to_delta[unit], unit=unit)
        return start, end

    def _resample(
        self,
        raw_data: pd.DataFrame,
        freq: Optional[str] = None,
    ) -> pd.DataFrame:
        raw_data["price"] = raw_data["price"] * raw_data["volume"]
        data: pd.DataFrame = raw_data.resample(freq).aggregate(
            self._bars_resample_funcs
        )
        data["price"] = data["price"] / data["volume"].replace(0, 1e-9)
        return data

    async def get(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: Optional[str] = None,
        lag: int = 0,
    ) -> pd.DataFrame:
        start, end = self._get_required_start_end(start, end, freq, lag)
        if start > end:
            columns = set(self._bars_resample_funcs.keys())
            df = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))
            return self.asset.localize(df)
        await self._update_data(start, end)
        data = self._bars.loc[start:end].copy()
        if freq is None:
            return data
        data = self._resample(data, freq)
        if lag == 0 or len(lag_data := data.loc[: start - pd.Timedelta(DTF)]) < lag:
            return data
        start = lag_data.index[-lag]
        return data.loc[start:end]

    async def get_sma(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 20,
        candle_key: str = "close",
    ) -> pd.Series:
        data = await self.get(start, end, freq, n - 1)
        return data[candle_key].dropna().rolling(n).mean()[start:]

    async def get_bollinger_bands(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 20,
        k: float = 2,
        candle_key: str = "close",
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        data = await self.get(start, end, freq, n - 1)
        rolling = data[candle_key].dropna().rolling(n)
        sma = rolling.mean()[start:]
        std = rolling.std()[start:]
        upper = sma + k * std
        lower = sma - k * std
        return lower, sma, upper

    async def get_atr(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 20,
    ) -> pd.Series:
        data = (await self.get(start, end, freq, n)).dropna()
        data["high_low"] = data["high"] - data["low"]
        prev_close = data["close"].shift(1)
        data["high_pc"] = (data["high"] - prev_close).abs()
        data["low_pc"] = (data["low"] - prev_close).abs()
        tr = data[["high_low", "high_pc", "low_pc"]].max(axis=1)
        return tr.rolling(n).mean()[start:]

    async def get_supertrend(
        self,
        start: pd.Timestamp,
        end: Optional[pd.Timestamp] = None,
        freq: str = None,
        n: int = 20,
        k: float = 2,
    ) -> pd.DataFrame:
        data = await self.get(start, end, freq, 1)
        cols = ["supertrend", "bullish"]
        default_result = pd.DataFrame(
            [],
            columns=cols,
            index=pd.DatetimeIndex([]),
            dtype=float,
        )
        default_result = self.asset.localize(default_result)
        if data.empty:
            return default_result
        data["atr"] = await self.get_atr(data.index[0], end, freq, n)
        data = data.dropna()
        if data.empty:
            return default_result
        hla = (data["high"] + data["low"]) / 2
        data["basic_upper"] = hla + k * data["atr"]
        data["basic_lower"] = hla - k * data["atr"]
        prev_row = data.iloc[0]
        prev_final_upper, prev_final_lower, prev_supertrend = (
            prev_row["basic_upper"],
            prev_row["basic_lower"],
            0,
        )
        supertrend = [(0, False)]
        for _, row in data.iloc[1:].iterrows():
            if (
                row["basic_upper"] < prev_final_upper
                or prev_row["close"] > prev_final_upper
            ):
                curr_final_upper = row["basic_upper"]
            else:
                curr_final_upper = prev_final_upper
            if (
                row["basic_lower"] > prev_final_lower
                or prev_row["close"] < prev_final_lower
            ):
                curr_final_lower = row["basic_lower"]
            else:
                curr_final_lower = prev_final_lower
            # Supertrend calculation
            if prev_supertrend == prev_final_upper:
                bullish = row["close"] > curr_final_upper
            else:
                bullish = row["close"] >= curr_final_lower
            curr_supertrend = curr_final_lower if bullish else curr_final_upper
            prev_final_upper, prev_final_lower, prev_supertrend = (
                curr_final_upper,
                curr_final_lower,
                curr_supertrend,
            )
            prev_row = row
            supertrend.append((curr_supertrend, bullish))
        df = pd.DataFrame(supertrend, columns=cols, index=data.index)
        df["bullish"] = df["bullish"].astype(bool)
        return df[df["supertrend"] != 0].loc[start:]


class RealTimeMixin:
    asset: Asset
    _lock: asyncio.Lock
    _bars: pd.DataFrame
    _subscribed: bool
    _new_value_event: asyncio.Event
    _retrieved_range: Tuple[pd.Timestamp, pd.Timestamp]
    _update_data: Callable[[pd.Timestamp, pd.Timestamp], Awaitable[None]]

    @abstractmethod
    async def _subscribe(self) -> None:
        """
        Subscribes to the bars realtime data. The subscription manager should use the add method
        to append data to the provider.
        """

    async def subscribe(self) -> None:
        async with self._lock:
            if self._subscribed:
                return
            await self._subscribe()
            now = self.asset.localize(pd.Timestamp.utcnow())
            curr_ts = now.floor(DTF) - pd.Timedelta(DTF)
            if (
                self._retrieved_range is not None
                and (curr_end := self._retrieved_range[1]) < curr_ts
            ):
                await self._update_data(curr_end, curr_ts)
            self._subscribed = True

    async def wait_for_next(self, freq: Optional[str] = None) -> pd.Series:
        await self._new_value_event.wait()
        self._new_value_event.clear()
        last_bar = self._bars.iloc[-1]
        if freq is None:
            return last_bar
        now: pd.Timestamp = last_bar.name
        start = now.floor(freq)
        end = start + pd.Timedelta(freq) - pd.Timedelta(DTF)
        while True:
            timeout = end.timestamp() - now.timestamp()
            try:
                if timeout <= 0:
                    break
                await asyncio.wait_for(
                    self._new_value_event.wait(),
                    timeout=timeout + 2,
                    # Wait up to 2 seconds after the current interval is over
                )
                last_bar = self._bars.iloc[-1]
                now = last_bar.name
            except asyncio.TimeoutError:
                break
            finally:
                self._new_value_event.clear()
        df = await self.get(start, end, freq=freq)
        return df.iloc[-1]


class RealTimeProvider(GenericBarsProvider, RealTimeMixin):
    ...
