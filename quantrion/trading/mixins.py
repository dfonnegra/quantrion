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
    ) -> Tuple[Optional[Order], Optional[Order]]:
        account = await asset.trader.get_account()
        size = get_risk_order_size(
            account.portfolio_value,
            account.buying_power,
            settings.GLOBAL_MAX_RISK_PERC,
            settings.GLOBAL_MAX_PORTFOLIO_PERC,
            risk,
            price,
        )
        if size <= 0:
            logger.warning(
                f"Tried to create order for {asset.symbol} with not enough buying power"
            )
            return (None, None)
        order_side = Side.BUY if long else Side.SELL
        order = await asset.trader.create_order(
            size,
            order_side,
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
        stop_price, profit_price = get_stop_profit_range(
            asset,
            order_side,
            self._win_to_loss_ratio,
            risk,
            executed_order.filled_price,
        )
        logger.info(
            f"Creating bracket orders for symbol {asset.symbol} with prices ({stop_price}, {profit_price})"
        )
        take_profit_order = await asset.trader.create_order(
            executed_order.filled_size,
            ~order_side,
            OrderType.LIMIT,
            price=profit_price,
        )
        stop_loss_order = await asset.trader.create_order(
            executed_order.filled_size,
            ~order_side,
            OrderType.STOP,
            price=stop_price,
        )
        tasks = [
            asyncio.create_task(asset.trader.wait_for_execution(take_profit_order.id)),
            asyncio.create_task(asset.trader.wait_for_execution(stop_loss_order.id)),
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        completed_order = done.pop().result()
        if completed_order.type == OrderType.LIMIT:
            await asset.trader.cancel_order(stop_loss_order.id)
        else:
            await asset.trader.cancel_order(take_profit_order.id)
        if (
            completed_order.status in [Status.CANCELLED, Status.REJECTED]
            or completed_order.filled_size < executed_order.filled_size
        ):
            logger.critical(
                f"Failed to execute stoploss/takeprofit order for {asset.symbol}. You'll have to manually close a {executed_order.type} position of {executed_order.filled_size} shares"
            )
        return (executed_order, completed_order)
