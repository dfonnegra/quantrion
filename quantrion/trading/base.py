from abc import ABC
from typing import Optional, Tuple, TypeVar, Union

from ..asset.base import TradableAsset
from .schemas import Account, Order, OrderType, Side, TimeInForce


class TradingError(Exception):
    pass


class OrderNotExecuted(TradingError):
    pass


class CancelOrderError(TradingError):
    pass


class TradingProvider(ABC):
    def __init__(self, asset: TradableAsset) -> None:
        self._asset = asset

    async def create_order(
        self,
        size: float,
        side: Side,
        type: OrderType,
        tif: TimeInForce = TimeInForce.GTC,
        price: Optional[Union[float, Tuple[float, float]]] = None,
    ) -> Order:
        ...

    async def cancel_order(self, order_id: str):
        ...

    async def wait_for_execution(
        self, order_id: str, timeout: Optional[float] = None
    ) -> Order:
        ...

    async def get_order(self, order_id: str) -> Order:
        ...

    async def get_account(self) -> Account:
        ...
