import asyncio
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
import pandas as pd
import pytz
import websockets


from .. import settings
from ..utils import SingletonMeta, retry_request, MarketDatetime as mdt
from .ticker import Ticker
from .fixtures import BarsFixture


FIELDS_TO_NAMES = {
    "t": "start",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "n": "n_trades",
    "vw": "price",
}


def _data_to_df(data: List[dict]) -> pd.DataFrame:
    if len(data) == 0:
        columns = set(FIELDS_TO_NAMES.values())
        columns.remove("start")
        return pd.DataFrame(
            columns=columns, index=pd.DatetimeIndex([], tz=mdt.market_tz)
        )
    df = pd.DataFrame(data).rename(columns=FIELDS_TO_NAMES)
    df["start"] = pd.to_datetime(df["start"].values).tz_convert(mdt.market_tz)
    return df.set_index("start")


class AlpacaWebSocket(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._socket = None
        self._cancelled = False
        self._task = None
        self._subscribed: Dict[str, AlpacaTicker] = dict()

    async def _subscribe_internal(self, symbols: List[str]):
        if len(symbols) == 0:
            return
        sleep_t = settings.DEFAULT_POLL_INTERVAL
        while self._socket is None:
            await asyncio.sleep(sleep_t)
            sleep_t = min(sleep_t * 2, 60)
        await self._socket.send(json.dumps({"action": "subscribe", "bars": symbols}))

    async def subscribe(self, ticker: "AlpacaTicker"):
        if self._task is None:
            self._task = asyncio.create_task(self.start())
        if ticker.symbol in self._subscribed:
            return
        await self._subscribe_internal([ticker.symbol])
        self._subscribed[ticker.symbol] = ticker

    async def start(self):
        async for sock in websockets.connect(settings.ALPACA_STREAMING_URL):
            self._socket = sock
            if self._cancelled:
                await sock.close()
                break
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
                        df = _data_to_df([data])
                        self._subscribed[symbol].add_bars(df)
            except websockets.ConnectionClosed:
                continue


class AlpacaTicker(Ticker, BarsFixture):
    _bars_resample_funcs = {
        **BarsFixture._bars_resample_funcs,
        "n_trades": "sum",
    }
    _bars_fill_values = {
        **BarsFixture._bars_fill_values,
        "n_trades": 0,
    }

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

    async def _retrieve_bars(
        self, start: datetime, end: Optional[datetime] = None
    ) -> pd.DataFrame:
        default_end = mdt.now()
        if settings.DEBUG:
            default_end -= timedelta(minutes=15)
        start = pd.Timestamp(start).ceil(settings.DEFAULT_TIMEFRAME)
        end = default_end if end is None else end
        end = pd.Timestamp(end).floor(settings.DEFAULT_TIMEFRAME)
        if start >= end:
            return _data_to_df([])
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
            return _data_to_df(rows)

    async def _subscribe_bars(self) -> None:
        ws = AlpacaWebSocket()
        await ws.subscribe(self)
