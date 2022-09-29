from abc import ABC, abstractmethod
from datetime import time
from typing import List

import pandas as pd
import pytz


class TradingRestriction(ABC):
    @abstractmethod
    def is_trading(self, at: pd.Timestamp = None) -> bool:
        pass

    @abstractmethod
    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class EmptyRestriction(TradingRestriction):
    def is_trading(self, at: pd.Timestamp = None) -> bool:
        return True

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


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

    def is_trading(self, at: pd.Timestamp = None) -> bool:
        if at is None:
            at = pd.Timestamp.now(tz=self._tz).time()
        if self._sgte:
            return self._end <= at <= self._start
        return (self._end <= at) or (at <= self._start)

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

    def is_trading(self, at: pd.Timestamp = None) -> bool:
        if at is None:
            at = pd.Timestamp.now(tz=self._tz)
        return at.dayofweek in self._days

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[~df.index.dayofweek.isin(self._days)]


class ComposedRestriction(TradingRestriction):
    def __init__(
        self,
        restrictions: List[TradingRestriction],
    ):
        self._restrictions = restrictions

    def is_trading(self, at: pd.Timestamp = None) -> bool:
        for r in self._restrictions:
            if not r.is_trading(at):
                return False
        return True

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        for r in self._restrictions:
            df = r.filter(df)
        return df
