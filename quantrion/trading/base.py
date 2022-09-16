from abc import ABC
from ctypes import Union
from typing import Optional, Tuple

from ..asset.base import TradableAsset
from .schemas import Order, OrderType, Side, TimeInForce


class TradingProvider(ABC):
    def __init__(self, asset: TradableAsset) -> None:
        self._asset = asset

    async def create_order(
        self,
        size: int,
        side: Side,
        type: OrderType,
        tif: TimeInForce = TimeInForce.GTC,
        price: Optional[Union[float, Tuple[float, float]]] = None,
    ) -> Order:
        ...

    async def cancel_order(self, order_id: str) -> Order:
        ...

    async def wait_for_execution(
        self, order_id: str, timeout: Optional[float] = None
    ) -> Order:
        ...

    async def get_order(self, order_id: str) -> Order:
        ...

    async def get_buying_power(self) -> float:
        ...

    async def get_portfolio_value(self) -> float:
        ...
