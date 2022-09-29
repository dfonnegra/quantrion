import asyncio
import json
import logging
from time import time
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union
from urllib.parse import urljoin

import httpx
import websockets

from .. import settings
from ..utils import MaxRetryError, SingletonMeta, retry_request
from .base import CancelOrderError, OrderNotExecuted, TradingError, TradingProvider
from .schemas import Account, Order, OrderType, Side, Status, TimeInForce

if TYPE_CHECKING:
    from ..asset.alpaca import AlpacaAsset

logger = logging.getLogger(__name__)


def _data_to_order(
    data: Dict[str, Any],
    org_type: OrderType,
    org_price: Optional[Union[float, Tuple[float, float]]],
) -> Order:
    status_map = {
        "new": Status.PENDING,
        "partially_filled": Status.PARTIALLY_FILLED,
        "filled": Status.FILLED,
        "done_for_day": Status.CANCELLED,
        "canceled": Status.CANCELLED,
        "expired": Status.CANCELLED,
        "replaced": Status.CANCELLED,
        "pending_cancel": Status.CANCELLED,
        "pending_replace": Status.CANCELLED,
        "accepted": Status.PENDING,
        "pending_new": Status.PENDING,
        "accepted_for_bidding": Status.PENDING,
        "stopped": Status.PENDING,
        "rejected": Status.REJECTED,
        "suspended": Status.REJECTED,
        "calculated": Status.CANCELLED,
    }
    filled_price = data.get("filled_avg_price")
    if filled_price is not None:
        filled_price = float(filled_price)
    return Order(
        id=data["id"],
        symbol=data["symbol"],
        size=float(data["qty"]),
        side=data["side"],
        type=org_type,
        tif=data["time_in_force"],
        price=org_price,
        status=status_map[data["status"]],
        filled_size=float(data["filled_qty"]),
        filled_price=filled_price,
    )


class AlpacaTradingWebSocket(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._socket = None
        self._task = None
        self._started = False
        self._order_id_to_provider: Dict[str, AlpacaTradingProvider] = dict()

    def subscribe(self, order_id: str, provider: "AlpacaTradingProvider") -> None:
        self._order_id_to_provider[order_id] = provider

    async def _start(self):
        async for sock in websockets.connect(settings.ALPACA_TRADING_WSS):
            self._socket = sock
            try:
                await sock.send(
                    json.dumps(
                        {
                            "action": "authenticate",
                            "data": {
                                "key_id": settings.ALPACA_API_KEY_ID,
                                "secret_key": settings.ALPACA_API_KEY_SECRET,
                            },
                        }
                    )
                )
                await self._socket.send(
                    json.dumps(
                        {"action": "listen", "data": {"streams": ["trade_updates"]}}
                    )
                )
                async for msg in sock:
                    message = json.loads(msg)
                    if message["stream"] == "listening":
                        self._started = True
                    if message["stream"] != "trade_updates":
                        continue
                    order_data = message["data"]["order"]
                    provider = self._order_id_to_provider.get(order_data["id"])
                    if provider is not None:
                        org_order = await provider.get_order(order_data["id"])
                        order = _data_to_order(
                            order_data, org_order.type, org_order.price
                        )
                        provider.update_order(order.id, order)
            except websockets.ConnectionClosed:
                continue

    async def start(self) -> None:
        if not self._task:
            self._task = asyncio.create_task(self._start())
            timeout = 10
            start = time()
            while time() - start < timeout:
                if self._started:
                    break
                await asyncio.sleep(settings.DEFAULT_POLL_INTERVAL)
            else:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    raise TimeoutError("Failed to start AlpacaTradingWebSocket")


class AlpacaTradingProvider(TradingProvider):
    def __init__(self, asset: "AlpacaAsset") -> None:
        super().__init__(asset)
        self._ws = AlpacaTradingWebSocket()
        self._id_to_order: Dict[str, Order] = dict()

    async def _request(
        self, method: str, path: str, json: Optional[dict] = None
    ) -> httpx.Response:
        async with httpx.AsyncClient() as client:
            headers = {
                "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
            }
            url = urljoin(settings.ALPACA_TRADING_URL, path)
            response = await retry_request(
                client, method, url, json=json, headers=headers
            )
            return response

    def update_order(self, order_id: str, order: Order):
        self._id_to_order[order_id] = order

    async def _get_stop_order_from_oco(self, oco_order: Order) -> Order:
        async with httpx.AsyncClient() as client:
            response = await retry_request(
                client,
                "GET",
                urljoin(settings.ALPACA_TRADING_URL, f"v2/orders"),
                params={"nested": True, "symbols": oco_order.symbol},
            )
            response.raise_for_status()
            data = response.json()
            for order in data:
                if order["id"] == oco_order.id:
                    return _data_to_order(
                        order["legs"][0], oco_order.type, oco_order.price
                    )
            raise TradingError("Failed to get stop order from OCO order")

    async def create_order(
        self,
        size: float,
        side: Side,
        type: OrderType,
        tif: TimeInForce = TimeInForce.GTC,
        price: Optional[Union[float, Tuple[float, float]]] = None,
    ) -> Order:
        await self._ws.start()
        asset: AlpacaAsset = self._asset
        try:
            if isinstance(price, float):
                price = asset.truncate_price(price)
            elif isinstance(price, tuple):
                price = (asset.truncate_price(price[0]), asset.truncate_price(price[1]))
            size = asset.truncate_size(size)
        except MaxRetryError as ex:
            raise OrderNotExecuted() from ex
        except httpx.HTTPStatusError as ex:
            raise OrderNotExecuted(ex.response.text) from ex
        data = {
            "symbol": self._asset.symbol,
            "qty": str(size),
            "side": side.value,
            "type": type.value,
            "time_in_force": tif.value,
        }
        if type == OrderType.LIMIT:
            data.update(limit_price=str(price))
        elif type == OrderType.STOP:
            data.update(stop_price=str(price))
        elif type == OrderType.STOP_LIMIT:
            data.update(stop_price=str(price[0]), limit_price=str(price[1]))
        elif type == OrderType.OCO:
            data.update(
                type=OrderType.LIMIT.value,
                order_class="oco",
                take_profit=dict(limit_price=str(price[1])),
                stop_loss=dict(stop_price=str(price[0])),
            )
        try:
            response = await self._request("post", "/v2/orders", json=data)
            response.raise_for_status()
        except MaxRetryError as ex:
            raise OrderNotExecuted() from ex
        except httpx.HTTPStatusError as ex:
            raise OrderNotExecuted(ex.response.text) from ex
        order = _data_to_order(response.json(), type, price)
        if order.type == OrderType.OCO:
            stop_order = await self._get_stop_order_from_oco(order)
            self.update_order(stop_order.id, stop_order)
            self._ws.subscribe(stop_order.id, self)
        self.update_order(order.id, order)
        self._ws.subscribe(order.id, self)
        return order

    async def cancel_order(self, order_id: str):
        try:
            response = await self._request("delete", f"/v2/orders/{order_id}")
            if response.status_code == 422:
                return
            response.raise_for_status()
        except (MaxRetryError, httpx.HTTPStatusError) as ex:
            logger.critical(
                "Failed to cancel order: %s. You'll need to cancel it manually",
                order_id,
            )
            raise CancelOrderError() from ex
        return

    async def wait_for_execution(
        self, order_id: str, timeout: Optional[float] = None, check_nested: bool = True
    ) -> Order:
        order = await self.get_order(order_id)
        if order.type == OrderType.OCO and check_nested:
            stop_order = await self._get_stop_order_from_oco(order)
            tasks = [
                asyncio.create_task(self.wait_for_execution(order.id, False)),
                asyncio.create_task(self.wait_for_execution(stop_order.id, False)),
            ]
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            return done.pop().result()
        start = time()
        is_timedout = lambda: timeout is None or time() - start < timeout
        while (
            order := await self.get_order(order_id)
        ).status == Status.PENDING or not is_timedout():
            await asyncio.sleep(settings.DEFAULT_POLL_INTERVAL)
        return order

    async def get_order(self, order_id: str) -> Order:
        return self._id_to_order[order_id]

    async def get_account(self) -> Account:
        try:
            response = await self._request("get", f"/v2/account")
            response.raise_for_status()
        except (MaxRetryError, httpx.HTTPStatusError) as ex:
            raise TradingError() from ex
        data = response.json()
        return Account(
            buying_power=float(data["buying_power"]),
            portfolio_value=float(data["portfolio_value"]),
        )
