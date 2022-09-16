from abc import ABC, ABCMeta
from typing import TYPE_CHECKING, Optional, TypeVar, Union

import pandas as pd

if TYPE_CHECKING:
    from ..data.base import RealTimeProvider, GenericBarsProvider
    from ..trading.base import TradingProvider

from .restriction import (
    ComposedRestriction,
    DayOfWeekRestriction,
    EmptyRestriction,
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
    _restriction: TradingRestriction = EmptyRestriction()
    _tz: str

    def __init__(
        self,
        symbol: str,
    ) -> None:
        self._symbol = symbol

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def restriction(self) -> Optional[TradingRestriction]:
        return self._restriction

    TS_OR_DF = TypeVar(
        "TS_OR_DF", pd.Timestamp, pd.Series, pd.DataFrame, pd.DatetimeIndex
    )

    def localize(self, ts_or_df: TS_OR_DF) -> TS_OR_DF:
        new_tz = getattr(self, "_tz", None)
        if isinstance(ts_or_df, (pd.DataFrame, pd.Series)):
            is_naive = ts_or_df.index.tz is None
        else:
            is_naive = ts_or_df.tz is None
        if is_naive and new_tz is None:
            return ts_or_df
        if is_naive:
            return ts_or_df.tz_localize("UTC").tz_convert(new_tz)
        return ts_or_df.tz_convert(new_tz)

    @property
    def is_trading(self) -> bool:
        return self.restriction.is_trading()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(symbol={self.symbol})"

    def __repr__(self) -> str:
        return str(self)


class TradableAsset(Asset):
    bars: "RealTimeProvider"
    trader: "TradingProvider"


class USStockMixin:
    _tz = "US/Eastern"
    _restriction = ComposedRestriction(
        [
            TimeRestriction("16:00", "09:30", _tz),
            DayOfWeekRestriction([5, 6], _tz),
        ]
    )
