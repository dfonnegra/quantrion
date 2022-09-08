from typing import List
from urllib.parse import urljoin

import httpx

from .. import settings
from ..data.alpaca import AlpacaCryptoBarsProvider, AlpacaUSStockBarsProvider
from ..data.base import AssetListProvider
from ..utils import SingletonMeta, retry_request
from .base import TradableAsset, USStockMixin


class AlpacaUSStock(TradableAsset, USStockMixin):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._bars = AlpacaUSStockBarsProvider(self)

    @property
    def bars(self) -> AlpacaUSStockBarsProvider:
        return self._bars


class AlpacaCrypto(TradableAsset):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._bars = AlpacaCryptoBarsProvider(self)

    @property
    def bars(self) -> AlpacaCryptoBarsProvider:
        return self._bars


class AlpacaUSStockListProvider(AssetListProvider, metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._cache = None
        super().__init__()

    async def list_assets(self) -> List[str]:
        if self._cache is not None:
            return self._cache
        async with httpx.AsyncClient() as client:
            url = urljoin(settings.ALPACA_TRADING_URL, f"/v2/assets")
            headers = {
                "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
            }
            params = {"status": "active", "asset_class": "us_equity"}
            response = await retry_request(
                client, "get", url, params=params, headers=headers
            )
            response.raise_for_status()
            result = [
                AlpacaUSStock(symbol=asset["symbol"])
                for asset in response.json()
                if asset["tradable"] and asset["fractionable"]
            ]
            self._cache = result
            return result
