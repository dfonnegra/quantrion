import logging
from typing import Optional

import pandas as pd

from ..asset.base import TradableAsset
from ..data.base import AssetListProvider
from ..trading.mixins import BasicTradeMixin
from .base import Strategy

logger = logging.getLogger(__name__)


class SupertrendStrategy(Strategy, BasicTradeMixin):
    def __init__(
        self,
        tl_provider: AssetListProvider,
        freq: Optional[str] = None,
        short_n: int = 20,
        short_k: float = 3.0,
        long_n: int = 40,
        long_k: float = 5.0,
        win_to_loss_ratio: float = 1.5,
    ) -> None:
        super().__init__(tl_provider, freq)
        self._short_n = short_n
        self._short_k = short_k
        self._long_n = long_n
        self._long_k = long_k
        self._win_to_loss_ratio = win_to_loss_ratio

    async def next(
        self,
        asset: TradableAsset,
        last_bar: pd.Series,
    ):
        start, end = last_bar.name, last_bar.name
        bars = await asset.bars.get(start, end, self._freq, lag=1)
        bars = asset.restriction.filter(bars)
        if len(bars) < 2:
            return
        st = await asset.bars.get_supertrend(
            bars.index[0], end, self._freq, self._short_n, self._short_k
        )
        lst = await asset.bars.get_supertrend(
            bars.index[0], end, self._freq, self._long_n, self._long_k
        )
        if len(lst) < 2:
            return
        st_bullish = st.iloc[-1]["bullish"]
        prev_st_bullish = st.iloc[-2]["bullish"]
        lst_bullish = lst.iloc[-1]["bullish"]
        if (
            st_bullish == prev_st_bullish
            or st_bullish
            and not lst_bullish
            or not st_bullish
            and lst_bullish
        ):
            return
        atr = await asset.bars.get_atr(start, end, self._freq, self._long_n)
        await self.trade(asset, last_bar["close"], atr.iloc[-1], st_bullish)
