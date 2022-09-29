from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx

from .. import settings
from ..data.alpaca import AlpacaBarsProvider, AlpacaUSStockBarsProvider
from ..data.base import AssetListProvider
from ..trading.alpaca import AlpacaTradingProvider
from ..utils import SingletonMeta, retry_request
from .base import TradableAsset, USStockMixin


class AlpacaAsset(TradableAsset):
    _bars: AlpacaBarsProvider

    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._trader = AlpacaTradingProvider(self)
        self._asset_data: Optional[Dict[str, Any]] = None

    async def get_asset_data(self) -> Dict[str, Any]:
        if self._asset_data is None:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    urljoin(settings.ALPACA_TRADING_URL, f"v2/assets/{self.symbol}"),
                    headers={
                        "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                        "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
                    },
                )
                response.raise_for_status()
                self._asset_data = response.json()
        return self._asset_data

    @property
    def bars(self) -> AlpacaBarsProvider:
        return self._bars

    @property
    def trader(self) -> AlpacaTradingProvider:
        return self._trader


class AlpacaUSStock(USStockMixin, AlpacaAsset):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._bars = AlpacaUSStockBarsProvider(self)


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
