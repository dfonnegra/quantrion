import asyncio
import base64
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
import pandas as pd
import pytz
import websockets

from .. import settings
from ..utils import MarketDatetime as mdt
from ..utils import SingletonMeta, retry_request
from .providers import BarsProvider
from .ticker import Ticker, TickerListProvider

BAR_FIELDS_TO_NAMES = {
    "t": "start",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "n": "n_trades",
    "vw": "price",
}


def _data_to_df(data: List[dict], field_to_names: Dict[str, str]) -> pd.DataFrame:
    if len(data) == 0:
        columns = set(field_to_names.values())
        columns.remove("start")
        return pd.DataFrame(
            columns=columns, index=pd.DatetimeIndex([], tz=mdt.market_tz)
        )
    df = pd.DataFrame(data).rename(columns=field_to_names)
    rm_cols = df.columns.difference(field_to_names.values())
    df.drop(rm_cols, axis=1, inplace=True)
    df["start"] = pd.to_datetime(df["start"].values).tz_convert(mdt.market_tz)
    return df.set_index("start")


class AlpacaWebSocket(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._socket = None
        self._task = None
        self._subscribed: Dict[str, AlpacaBarsProvider] = dict()

    async def _subscribe_internal(self, symbols: List[str]):
        if len(symbols) == 0:
            return
        sleep_t = settings.DEFAULT_POLL_INTERVAL
        while self._socket is None:
            await asyncio.sleep(sleep_t)
            sleep_t = min(sleep_t * 2, 60)
        await self._socket.send(json.dumps({"action": "subscribe", "bars": symbols}))

    async def subscribe(self, bars: "AlpacaBarsProvider"):
        if self._task is None:
            self._task = asyncio.create_task(self.start())
        if bars.symbol in self._subscribed:
            return
        await self._subscribe_internal([bars.symbol])
        self._subscribed[bars.symbol] = bars

    async def start(self):
        async for sock in websockets.connect(settings.ALPACA_STREAMING_URL):
            self._socket = sock
            try:
                await sock.send(
                    json.dumps(
                        {
                            "action": "auth",
                            "key": settings.ALPACA_API_KEY_ID,
                            "secret": settings.ALPACA_API_KEY_SECRET,
                        }
                    )
                )
                symbols = list(self._subscribed.keys())
                await self._subscribe_internal(symbols)
                async for msg in sock:
                    messages = json.loads(msg)
                    for data in messages:
                        if (symbol := data.get("S")) is None:
                            continue
                        df = _data_to_df([data], BAR_FIELDS_TO_NAMES)
                        self._subscribed[symbol].add(df)
            except websockets.ConnectionClosed:
                continue


class AlpacaBarsProvider(BarsProvider):
    async def _next_page(
        self,
        client: httpx.AsyncClient,
        *args,
        next_token: Optional[str] = None,
        **kwargs,
    ) -> httpx.Response:
        if next_token is not None:
            kwargs["params"]["page_token"] = next_token
        response = await retry_request(client, "get", *args, **kwargs)
        response.raise_for_status()
        return response

    async def _retrieve(self, start: datetime, end: datetime) -> pd.DataFrame:
        dt = mdt.now() - end
        dt_min = timedelta(minutes=15)
        if settings.DEBUG and dt < dt_min:
            end = end - dt_min
        if start >= end:
            return _data_to_df([], BAR_FIELDS_TO_NAMES)
        async with httpx.AsyncClient() as client:
            url = urljoin(settings.ALPACA_DATA_URL, f"/v2/stocks/{self.symbol}/bars")
            headers = {
                "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
            }
            params = {
                "timeframe": settings.DEFAULT_TIMEFRAME,
                "start": start.astimezone(pytz.UTC).isoformat(),
                "end": end.astimezone(pytz.UTC).isoformat(),
            }
            response = await self._next_page(
                client, url, params=params, headers=headers
            )
            data = response.json()
            rows: list = data.get("bars", []) or []
            while (token := data.get("next_page_token")) is not None:
                response = await self._next_page(
                    client, url, params=params, headers=headers, next_token=token
                )
                data = response.json()
                rows.extend(data.get("bars", []))
            return _data_to_df(rows, BAR_FIELDS_TO_NAMES)

    async def _subscribe(self) -> None:
        ws = AlpacaWebSocket()
        await ws.subscribe(self)


class AlpacaTicker(Ticker):
    _bars_resample_funcs = {
        **BarsProvider._bars_resample_funcs,
        "n_trades": "sum",
    }
    _bars_fill_values = {
        **BarsProvider._bars_fill_values,
        "n_trades": 0,
    }

    def __init__(self, symbol: str) -> None:
        super().__init__(symbol)
        self._bars = AlpacaBarsProvider(symbol)

    @property
    def bars(self) -> BarsProvider:
        return self._bars


class AlpacaTickerListProvider(TickerListProvider, metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._cache = None
        super().__init__()

    async def list_tickers(self) -> List[str]:
        if self._cache is not None:
            return self._cache
        async with httpx.AsyncClient() as client:
            url = urljoin(settings.ALPACA_TRADING_URL, f"/v2/assets")
            headers = {
                "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
            }
            params = {"status": "active", "asset_class": "us_equity"}
            response = await retry_request(
                client, "get", url, params=params, headers=headers
            )
            response.raise_for_status()
            result = [
                AlpacaTicker(symbol=asset["symbol"])
                for asset in response.json()
                if asset["tradable"] and asset["fractionable"]
            ]
            self._cache = result
            return result
