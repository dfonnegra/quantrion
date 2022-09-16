import math


def get_risk_order_size(
    portfolio_value: float,
    buying_power: float,
    bp_percent: float,
    risk: float,
    price: float,
):
    size = math.floor(bp_percent * portfolio_value / risk)
    max_size = int(0.5 * buying_power / price)
    return min(size, max_size)


def get_stop_profit_range(
    win_to_loss_ratio: float,
    risk: float,
    price: float,
):
    return (price - risk, (price + win_to_loss_ratio * risk) * price / (price - risk))
