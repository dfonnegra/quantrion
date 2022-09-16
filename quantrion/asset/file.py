import asyncio

import pandas as pd

from ..data.base import RealTimeProvider
from .base import TradableAsset, USStockMixin


class CSVProvider(RealTimeProvider):
    def __init__(self, asset: TradableAsset, path: str) -> None:
        super().__init__(asset)
        self._path = path
        bars = pd.read_csv(self._path)
        bars["start"] = pd.DatetimeIndex(pd.to_datetime(bars["start"], utc=True))
        self._df = self.asset.localize(bars.set_index("start"))
        self._curr_idx = -1
        self._task: asyncio.Task = None

    async def _retrieve(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        return self._df.iloc[: self._curr_idx + 1].loc[start:end]

    async def _subscribe(self) -> None:
        async def start():
            while True:
                self._curr_idx += 1
                df = self._df.iloc[self._curr_idx : self._curr_idx + 1]
                if df.empty:
                    break
                self.add(df)
                while self._new_value_event.is_set():
                    await asyncio.sleep(0)

        self._task = asyncio.create_task(start())


class CSVAsset(TradableAsset):
    def __init__(self, symbol: str, path: str) -> None:
        super().__init__(symbol)
        self._bars = CSVProvider(self, path)
        self._trader = None

    @property
    def bars(self) -> CSVProvider:
        return self._bars


class CSVUSStock(CSVAsset, USStockMixin):
    pass
