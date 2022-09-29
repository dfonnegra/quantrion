from enum import Enum
from typing import Optional, Tuple, Union

from pydantic import BaseModel, Field


class Side(Enum):
    BUY = "buy"
    SELL = "sell"

    def __invert__(self):
        return Side.BUY if self == Side.SELL else Side.SELL


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    OCO = "oco"


class TimeInForce(Enum):
    DAY = "day"
    GTC = "gtc"
    OPG = "opg"
    CLS = "cls"


class Status(Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class Order(BaseModel):
    id: str
    symbol: str
    size: float
    side: Side
    type: OrderType
    tif: TimeInForce
    price: Optional[Union[float, Tuple[float, float]]] = Field(None)
    status: Status
    filled_size: float
    filled_price: Optional[float] = Field(None)


class Account(BaseModel):
    buying_power: float
    portfolio_value: float
