from enum import Enum
from typing import Optional, Tuple, Union

from pydantic import BaseModel, Field


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    RANGE = "RANGE_ORDER"


class TimeInForce(Enum):
    DAY = "DAY"
    GTC = "GTC"
    OPG = "OPG"
    CLS = "CLS"


class Status(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Order(BaseModel):
    id: str
    symbol: str
    size: int
    side: Side
    type: OrderType
    tif: TimeInForce
    price: Optional[Union[float, Tuple[float, float]]] = Field(None)
    status: Status
    filled_size: int
    filled_price: Optional[float] = Field(None)
