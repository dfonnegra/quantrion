import math
from typing import Tuple

from ..asset.base import TradableAsset
from ..trading.schemas import Side


def get_risk_order_size(
    portfolio_value: float,
    buying_power: float,
    portfolio_perc: float,
    max_portfolio_perc: float,
    risk: float,
    price: float,
) -> float:
    size = portfolio_perc / 100 * portfolio_value / risk
    max_size = max_portfolio_perc / 100 * portfolio_value / price
    return min(size, max_size, buying_power)


def get_stop_profit_range(
    asset: TradableAsset,
    side: Side,
    win_to_loss_ratio: float,
    risk: float,
    price: float,
) -> Tuple[float, float]:
    low_price = asset.truncate_price(price - risk)
    high_price = asset.truncate_price(
        (price + win_to_loss_ratio * risk) * price / (price - risk)
    )
    if side == Side.BUY:
        return (low_price, high_price)
    return (high_price, low_price)
