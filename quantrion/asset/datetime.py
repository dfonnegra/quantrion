from abc import ABC, abstractmethod
from datetime import time
from typing import List

import pandas as pd
import pytz


class TradingRestriction(ABC):
    @abstractmethod
    def is_trading(self) -> bool:
        pass

    @abstractmethod
    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class TimeRestriction(TradingRestriction):
    def __init__(
        self,
        start: str,
        end: str,
        tz: str = "UTC",
    ):
        self._start = time.fromisoformat(start)
        self._end = time.fromisoformat(end)
        self._sgte = start > end
        self._tz = tz

    def is_trading(self) -> bool:
        now = pd.Timestamp.now(tz=self._tz).time()
        if self._sgte:
            return self._end <= now <= self._start
        return (self._end <= now) or (now <= self._start)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._sgte:
            return df[(self._end <= df.index.time) & (df.index.time <= self._start)]
        return df[(df.index.time <= self._start) | (self._end <= df.index.index)]


class DayOfWeekRestriction(TradingRestriction):
    def __init__(
        self,
        days: List[int],
        tz: str = "UTC",
    ):
        self._days = days
        self._tz = tz

    def is_trading(self) -> bool:
        return pd.Timestamp.now(tz=self._tz).dayofweek in self._days

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[~df.index.dayofweek.isin(self._days)]


class ComposedRestriction(TradingRestriction):
    def __init__(
        self,
        restrictions: List[TradingRestriction],
    ):
        self._restrictions = restrictions

    def is_trading(self) -> bool:
        for r in self._restrictions:
            if not r.is_trading():
                return False
        return True

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        for r in self._restrictions:
            df = r.filter(df)
        return df


class AssetDatetime:
    """
    Auxuliary class that contains market datetime helpers.
    """

    def __init__(self, tz: str = "UTC"):
        self._tz = tz

    def from_timestamp(self, timestamp: float) -> pd.Timestamp:
        """
        Retrieves the datetime from a timestamp localized to the asset timezone.

        Args:
            timestamp (:obj:`float`): The timestamp in seconds to convert.

        Returns:
            :obj:`pd.Timestamp`: The datetime localized to the asset timezone.
        """
        dt = pd.Timestamp.fromtimestamp(timestamp, tz=pytz.UTC)
        return dt.astimezone(self._tz)

    def now(self) -> pd.Timestamp:
        """
        Returns:
            :obj:`pd.Timestamp`: The current datetime localized to the market timezone.
        """
        return pd.Timestamp.utcnow().astimezone(self._tz)
