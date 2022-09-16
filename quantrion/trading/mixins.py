import asyncio
import logging
from typing import Optional, Tuple

from .. import settings
from ..asset.base import TradableAsset
from ..strategy.func import get_risk_order_size, get_stop_profit_range
from .schemas import Order, OrderType, Side, Status

logger = logging.getLogger(__name__)


class BasicTradeMixin:
    _win_to_loss_ratio: float

    async def trade(
        self,
        asset: TradableAsset,
        price: float,
        risk: float,
        long: bool,
    ) -> Tuple[Order, Optional[Order]]:
        buying_power = await asset.trader.get_buying_power()
        portfolio_value = await asset.trader.get_portfolio_value()
        size = get_risk_order_size(
            portfolio_value, buying_power, settings.GLOBAL_MAX_RISK, risk, price
        )
        if size <= 0:
            logger.warning(
                f"Tried to create order for {asset.symbol} with not enough buying power"
            )
            return
        order = await asset.trader.create_order(
            size,
            Side.BUY if long else Side.SELL,
            OrderType.MARKET,
        )
        executed_order = await asset.trader.wait_for_execution(order.id, timeout=60)
        if (
            executed_order.status in [Status.CANCELLED, Status.REJECTED]
            and executed_order.filled_size == 0
        ):
            return (executed_order, None)
        if executed_order.status == Status.PENDING:
            await asset.trader.cancel_order(executed_order.id)
            return (executed_order, None)
        if executed_order.status == Status.PARTIALLY_FILLED:
            await asset.trader.cancel_order(executed_order.id)
        stop_profit_range = get_stop_profit_range(
            self._win_to_loss_ratio, risk, executed_order.filled_price
        )
        range_order = await asset.trader.create_order(
            executed_order.filled_size,
            Side.SELL if long else Side.BUY,
            OrderType.RANGE,
            price=stop_profit_range,
        )
        executed_range_order = await asset.trader.wait_for_execution(range_order.id)
        if (
            executed_range_order.status in [Status.CANCELLED, Status.REJECTED]
            or executed_range_order.filled_size == 0
        ):
            logger.error(
                f"Failed to execute stoploss/takeprofit order for {asset.symbol}. You'll have to manually close a {executed_order.type} position of {executed_order.filled_size} shares"
            )
        return (executed_order, executed_range_order)
