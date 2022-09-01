from abc import ABC, ABCMeta, abstractmethod
from typing import List, Optional

import pandas as pd

from .datetime import (
    AssetDatetime,
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
        restriction: Optional[TradingRestriction] = None,
        tz: str = "UTC",
    ) -> None:
        self._symbol = symbol
        self._tz = tz
        self._restriction = restriction
        self._dt = AssetDatetime(tz=tz)

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

    def is_trading_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.is_restricted:
            return df
        return self._restriction.filter(df)

    @property
    def dt(self) -> AssetDatetime:
        return self._dt

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(symbol={self.symbol})"

    def __repr__(self) -> str:
        return str(self)


class USStock(Asset):
    def __init__(
        self,
        symbol: str,
    ) -> None:
        tz = "US/Eastern"
        restriction = ComposedRestriction(
            [
                TimeRestriction("16:00", "09:30", tz),
                DayOfWeekRestriction([5, 6], tz),
            ]
        )
        super().__init__(symbol, restriction, tz)


class Crypto(Asset):
    def __init__(
        self,
        symbol: str,
    ) -> None:
        super().__init__(symbol, tz="UTC")


class AssetListProvider(ABC):
    @abstractmethod
    async def list_assets(self) -> List[Asset]:
        pass
