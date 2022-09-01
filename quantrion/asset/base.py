from abc import ABC, ABCMeta, abstractmethod
from datetime import time
from typing import List, Optional, Tuple

import pandas as pd

from quantrion.asset.datetime import (
    ComposedRestriction,
    DayOfWeekRestriction,
    TimeRestriction,
    TradingRestriction,
)


class AssetMeta(ABCMeta):
    _instances = None
    _classes = []

    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        mcs._classes.append(cls)
        return cls

    def __call__(cls, symbol: str, *args, **kwargs):
        if cls._instances is None:
            cls._instances = {}
        if symbol not in cls._instances:
            cls._instances[symbol] = super().__call__(symbol, *args, **kwargs)
        return cls._instances[symbol]


class Asset(ABC, metaclass=AssetMeta):
    def __init__(
        self,
        symbol: str,
        tz: str = "UTC",
        restriction: Optional[TradingRestriction] = None,
    ) -> None:
        self._symbol = symbol
        self._tz = tz
        self._restriction = restriction

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def restriction(self) -> Optional[TradingRestriction]:
        return self._restriction

    @property
    def is_restricted(self) -> bool:
        return self.restriction is not None

    @property
    def tz(self) -> str:
        return self._tz

    @property
    def is_trading(self) -> bool:
        if self.is_restricted:
            return self.restriction.is_trading()
        return True

    @property
    def filter_market_open(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.is_restricted:
            return df
        return self._restriction.filter(df)


class USStock(Asset):
    @property
    def symbol(self) -> str:
        return self._symbol

    def __init__(
        self,
        symbol: str,
    ) -> None:
        self._tz = "US/Eastern"
        restriction = ComposedRestriction(
            [
                TimeRestriction("16:00", "09:30", self._tz),
                DayOfWeekRestriction([5, 6], self._tz),
            ]
        )
        super().__init__(symbol, restriction)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(symbol={self.symbol})"

    def __repr__(self) -> str:
        return str(self)


class AssetListProvider(ABC):
    @abstractmethod
    async def list_assets(self) -> List[Asset]:
        pass
