from typing import Optional, Protocol

import pandas as pd

from ..asset.restriction import TradingRestriction
from ..data.base import AssetListProvider, RealTimeProvider
from .base import Position, Strategy


class SupertrendAsset(Protocol):
    bars: RealTimeProvider
    restriction: Optional[TradingRestriction]
    is_restricted: bool


class SupertrendStrategy(Strategy):
    def __init__(
        self,
        tl_provider: AssetListProvider,
        freq: Optional[str] = None,
        short_n: int = 20,
        short_k: float = 3.0,
        long_n: int = 40,
        long_k: float = 5.0,
    ) -> None:
        super().__init__(tl_provider)
        self._freq = freq
        self._short_n = short_n
        self._short_k = short_k
        self._long_n = long_n
        self._long_k = long_k

    async def run_for_asset(self, asset: SupertrendAsset):
        last_position = Position.FLAT
        while True:
            last_bar = await asset.bars.wait_for_next(self._freq)
            position = await self.evaluate_position(asset, last_position, last_bar)
            # TODO: Implement position management
            last_position = position

    async def evaluate_position(
        self,
        asset: SupertrendAsset,
        last_position: Position,
        last_bar: pd.Series,
    ) -> Position:
        start, end = last_bar.name, last_bar.name
        bars = await asset.bars.get(start, end, self._freq, lag=1)
        if asset.is_restricted:
            bars = asset.restriction.filter(bars)
        if len(bars) < 2:
            return last_position
        st = await asset.bars.get_supertrend(
            bars.index[0], end, self._freq, self._short_n, self._short_k
        )
        long_st = await asset.bars.get_supertrend(
            bars.index[0], end, self._freq, self._long_n, self._long_k
        )
        if len(long_st) < 2:
            return last_position
        supertrend, bullish = supertrend["supertrend"], supertrend["bullish"]
        long_supertrend, long_bullish = long_st["supertrend"], long_st["bullish"]

        prev_st_value = long_st.iloc[-2]
        st_value, st_bullish = st.iloc[-1]["supertrend"], st.iloc[-1]["bullish"]
        lst_value, lst_bullish = (
            long_st.iloc[-1]["supertrend"],
            long_st.iloc[-1]["bullish"],
        )

        atr = await asset.bars.get_atr(start, end, self._freq, self._short_n)
        return Position.FLAT
