from typing import List, Type, TypeVar

import pandas as pd

from ..asset.base import Asset
from .base import AssetListProvider

AssetSubclass = TypeVar("AssetSubclass", bound=Asset)


class CSVAssetListProvider(AssetListProvider):
    def __init__(self, file_path, asset_class: Type[AssetSubclass] = None) -> None:
        self._file_path = file_path
        self._AssetClass = asset_class

    async def list_assets(self) -> List[AssetSubclass]:
        df = pd.read_csv(self._file_path)
        return [self._AssetClass(symbol) for symbol in df["symbol"]]
