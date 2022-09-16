from typing import Dict, List, Optional, Tuple, Union
from uuid import uuid4

from ..asset.base import TradableAsset
from .base import TradingProvider
from .schemas import Order, OrderType, Side, Status, TimeInForce


class BacktestTradingProvider(TradingProvider):
    def __init__(self, asset: TradableAsset) -> None:
        super().__init__(asset)
        self._orders: Dict[str, List[Order]] = {}
        self._positions: Dict[str, int] = {}

    async def create_order(
        self,
        size: int,
        side: Side,
        type: OrderType,
        tif: TimeInForce = TimeInForce.GTC,
        price: Optional[Union[float, Tuple[float, float]]] = None,
    ) -> Order:
        order_id = uuid4().hex
        order = Order(
            id=order_id,
            symbol=self._asset.symbol,
            size=size,
            side=side,
            type=type,
            tif=tif,
            price=price,
            status=Status.PENDING,
            filled_size=0,
            filled_price=None,
        )
        self._orders[order_id] = order
        return order

    async def cancel_order(self, order_id: str) -> Order:
        new_order = self._orders[order_id].copy(update={"status": Status.CANCELLED})
        return new_order

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
