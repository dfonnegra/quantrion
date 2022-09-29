import logging
import math
from typing import Optional

import numpy as np
import pandas as pd

from ..asset.base import TradableAsset
from ..data.base import AssetListProvider
from ..trading.base import TradingError
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
        volume_k_std: float = 1.5,
        risk_multiplier: float = 1.0,
        win_to_loss_ratio: float = 1.5,
    ) -> None:
        super().__init__(tl_provider, freq)
        self._short_n = short_n
        self._short_k = short_k
        self._long_n = long_n
        self._long_k = long_k
        self._volume_k_std = volume_k_std
        self._risk_multiplier = risk_multiplier
        self._win_to_loss_ratio = win_to_loss_ratio

    async def next(
        self,
        asset: TradableAsset,
        last_bar: pd.Series,
    ):
        if not asset.is_trading(last_bar.name):
            return
        start, end = last_bar.name, last_bar.name
        print("Original (start, end)")
        print(start, end)
        bars = await asset.bars.get(start, end, self._freq, lag=self._long_n)
        if len(bars) < 2:
            return
        volume = bars["volume"].astype(float)
        mean_log_vol = np.log(volume + 1e-3).iloc[:-1].mean()
        std_log_vol = np.log(volume + 1e-3).iloc[:-1].std()
        log_vol = np.log(volume.iloc[-1] + 1e-3)
        # if log_vol < mean_log_vol + self._volume_k_std * std_log_vol:
        #     return
        st = await asset.bars.get_supertrend(
            start, end, self._freq, self._short_n, self._short_k
        )
        lst = await asset.bars.get_supertrend(
            start, end, self._freq, self._long_n, self._long_k
        )
        print("Supertrend")
        print(st)
        print("Long Supertrend")
        print(lst)
        if lst.empty:
            return
        st_bullish = st.iloc[-1]["bullish"]
        lst_bullish = lst.iloc[-1]["bullish"]
        if st_bullish and not lst_bullish or not st_bullish and lst_bullish:
            return
        atr = await asset.bars.get_atr(start, end, self._freq, self._long_n)
        risk = self._risk_multiplier * atr.iloc[-1]
        logger.info(
            f"{self.__class__.__name__} will open position for {asset.symbol} with bar:\n {last_bar}"
        )
        try:
            await self.trade(asset, last_bar["close"], risk, st_bullish)
        except TradingError as e:
            logger.exception(e)
