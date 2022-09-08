import pandas as pd

from ..data.base import GenericBarsProvider
from .base import Asset, BacktestableAsset, USStockMixin


class CSVProvider(GenericBarsProvider):
    def __init__(self, asset: Asset, path: str) -> None:
        super().__init__(asset)
        self._path = path
        self._df = None

    async def _retrieve(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        if self._df is None:
            bars = pd.read_csv(self._path)
            bars["start"] = pd.DatetimeIndex(pd.to_datetime(bars["start"], utc=True))
            self._df = self.asset.localize(bars.set_index("start"))
        return self._df.loc[start:end]


class CSVAsset(BacktestableAsset):
    def __init__(self, symbol: str, path: str) -> None:
        super().__init__(symbol)
        self._bars = CSVProvider(self, path)

    @property
    def bars(self) -> CSVProvider:
        return self._bars


class CSVUSStock(CSVAsset, USStockMixin):
    pass
