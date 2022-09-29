from typing import List
from urllib.parse import urljoin

import httpx

from .. import settings
from ..data.alpaca import (
    AlpacaBarsProvider,
    AlpacaCryptoBarsProvider,
    AlpacaUSStockBarsProvider,
)
from ..data.base import AssetListProvider
from ..trading.alpaca import AlpacaTradingProvider
from ..utils import SingletonMeta, retry_request
from .base import TradableAsset, USStockMixin


class AlpacaAsset(TradableAsset):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._trader = AlpacaTradingProvider(self)

    @property
    def bars(self) -> AlpacaBarsProvider:
        return self._bars

    @property
    def trader(self) -> AlpacaTradingProvider:
        return self._trader


class AlpacaUSStock(AlpacaAsset, USStockMixin):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._bars = AlpacaUSStockBarsProvider(self)


class AlpacaCrypto(AlpacaAsset):
    def __init__(self, symbol: str, min_trade_increment: float) -> None:
        super().__init__(symbol)
        self._bars = AlpacaCryptoBarsProvider(self)
        self._min_trade_increment = min_trade_increment


class AlpacaUSStockListProvider(AssetListProvider, metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._cache = None
        super().__init__()

    async def list_assets(self) -> List[AlpacaUSStock]:
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
