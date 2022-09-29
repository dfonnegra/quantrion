import asyncio
import json
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
import pandas as pd
import pytz
import websockets

from .. import settings
from ..asset.base import Asset
from ..utils import SingletonMeta, retry_request
from .base import RealTimeProvider

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


def _data_to_df(
    data: List[dict], field_to_names: Dict[str, str], asset: Asset
) -> pd.DataFrame:
    if len(data) == 0:
        columns = set(field_to_names.values())
        columns.remove("start")
        index = asset.localize(pd.DatetimeIndex([], name="start"))
        return pd.DataFrame(columns=columns, index=index)
    df = pd.DataFrame(data).rename(columns=field_to_names)
    rm_cols = df.columns.difference(field_to_names.values())
    df.drop(rm_cols, axis=1, inplace=True)
    df["start"] = pd.to_datetime(df["start"].values).tz_convert(None)
    return asset.localize(df.set_index("start"))


class AlpacaWebSocket(metaclass=SingletonMeta):
    def __init__(self, url: str) -> None:
        self._socket = None
        self._task = None
        self._symbol_to_provider: Dict[str, AlpacaBarsProvider] = dict()
        self._url = url

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
        if bars.asset.symbol in self._symbol_to_provider:
            return
        await self._subscribe_internal([bars.asset.symbol])
        self._symbol_to_provider[bars.asset.symbol] = bars

    async def start(self):
        async for sock in websockets.connect(self._url):
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
                symbols = list(self._symbol_to_provider.keys())
                await self._subscribe_internal(symbols)
                async for msg in sock:
                    messages = json.loads(msg)
                    for data in messages:
                        if (symbol := data.get("S")) is None:
                            continue
                        provider = self._symbol_to_provider[symbol]
                        df = _data_to_df([data], BAR_FIELDS_TO_NAMES, provider.asset)
                        provider.add(df)
            except websockets.ConnectionClosed:
                continue


class AlpacaUSStockWebSocket(AlpacaWebSocket):
    def __init__(self) -> None:
        super().__init__(urljoin(settings.ALPACA_STREAMING_URL, f"/v2/sip"))


class AlpacaBarsProvider(RealTimeProvider):
    _bars_resample_funcs = {
        **RealTimeProvider._bars_resample_funcs,
        "n_trades": "sum",
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

    async def _retrieve(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        if start >= end:
            return _data_to_df([], BAR_FIELDS_TO_NAMES, self.asset)
        async with httpx.AsyncClient() as client:
            url = self._get_historical_url()
            headers = {
                "APCA-API-KEY-ID": settings.ALPACA_API_KEY_ID,
                "APCA-API-SECRET-KEY": settings.ALPACA_API_KEY_SECRET,
            }
            if start.tz is None:
                start = start.tz_localize(pytz.UTC)
                end = end.tz_localize(pytz.UTC)
            else:
                start = start.astimezone(pytz.UTC)
                end = end.astimezone(pytz.UTC)
            params = {
                "timeframe": settings.DEFAULT_TIMEFRAME,
                "start": start.isoformat(),
                "end": end.isoformat(),
                **self._get_extra_params(),
            }
            response = await self._next_page(
                client, url, params=params, headers=headers
            )
            next_token, rows = self._process_response(response)
            while next_token is not None:
                response = await self._next_page(
                    client, url, params=params, headers=headers, next_token=next_token
                )
                next_token, new_rows = self._process_response(response)
                rows.extend(new_rows)
            return _data_to_df(rows, BAR_FIELDS_TO_NAMES, self.asset)

    @abstractmethod
    def _get_historical_url(self) -> str:
        pass

    def _get_extra_params(self) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def _get_web_socket(self) -> AlpacaWebSocket:
        pass

    @abstractmethod
    def _process_response(self, response: httpx.Response) -> Tuple[Optional[str], list]:
        pass

    async def _subscribe(self) -> None:
        ws = self._get_web_socket()
        await ws.subscribe(self)


class AlpacaUSStockBarsProvider(AlpacaBarsProvider):
    def _get_historical_url(self) -> str:
        return urljoin(settings.ALPACA_DATA_URL, f"/v2/stocks/{self.asset.symbol}/bars")

    def _get_extra_params(self) -> Dict[str, Any]:
        return {"adjustment": "all"}

    def _get_web_socket(self) -> AlpacaUSStockWebSocket:
        return AlpacaUSStockWebSocket()

    def _process_response(self, response: httpx.Response) -> Tuple[Optional[str], list]:
        data = response.json()
        return data.get("next_page_token"), data.get("bars", []) or []
