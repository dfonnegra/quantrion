import asyncio
import math
from datetime import datetime, timedelta
from random import random
from typing import Callable, Optional
from urllib.parse import urlencode, urljoin

import pandas as pd
import pytz
from httpx import RequestError
from pytest_httpx import HTTPXMock

from quantrion import settings
from quantrion.ticker.alpaca import BAR_FIELDS_TO_NAMES, AlpacaTicker, AlpacaWebSocket
from quantrion.utils import MarketDatetime as mdt


def normalize_start_end(start: datetime, end: Optional[datetime] = None):
    start = pd.Timestamp(start.astimezone(pytz.UTC)).ceil(settings.DEFAULT_TIMEFRAME)
    end = mdt.now() if end is None else end
    end = pd.Timestamp(end).floor(settings.DEFAULT_TIMEFRAME)
    return start, end.astimezone(pytz.UTC)


def get_bars_url(symbol: str, start: datetime, end: Optional[datetime] = None):
    start, end = normalize_start_end(start, end)
    params = {
        "timeframe": settings.DEFAULT_TIMEFRAME,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    url = urljoin(settings.ALPACA_DATA_URL, f"/v2/stocks/{symbol}/bars")
    return url + f"?{urlencode(params)}"


def generate_bars(start: datetime, end: Optional[datetime] = None):
    start, end = normalize_start_end(start, end)
    dates = pd.date_range(start, end, freq=settings.DEFAULT_TIMEFRAME)
    return [
        {
            "t": dt.isoformat().replace("+00:00", "Z"),
            "o": (open_ := 100 + random() * 20),
            "c": (close := 100 + random() * 20),
            "h": (max(open_, close) + random() * 20),
            "l": (min(open_, close) - random() * 20),
            "v": random() * 100000,
            "n": random() * 100,
            "vw": (open_ + close) / 2,
        }
        for dt in dates
    ]


async def test_get_bars_empty(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() - timedelta(days=1)
    end = mdt.now()
    url = get_bars_url("AAPL", start, end)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": [],
        },
    )
    bars = await ticker.get_bars(start, end)
    assert bars.shape[0] == 0


async def test_get_bars_no_end(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    now = mdt.now() - timedelta(minutes=5)
    url = get_bars_url("AAPL", now)
    expected_bars = generate_bars(now)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": expected_bars,
        },
    )
    expected_bars = [
        {BAR_FIELDS_TO_NAMES[key]: value for key, value in bar.items() if key != "t"}
        for bar in expected_bars
    ]
    bars = await ticker.get_bars(now)
    assert len(bars) == len(expected_bars)
    assert bars.to_dict("records") == expected_bars


async def test_get_bars_update(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    start1 = mdt.now() - timedelta(days=4)
    end1 = mdt.now() - timedelta(days=3, hours=1)
    start2 = mdt.now() - timedelta(days=3)
    url = get_bars_url("AAPL", start1, end1)
    bars1 = generate_bars(start1, end1)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars1,
        },
    )
    result = await ticker.get_bars(start1, end1)
    url = get_bars_url("AAPL", result.index[-1])
    bars2 = generate_bars(result.index[-1])
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars2,
        },
    )
    await ticker.get_bars(start1, end1)
    await ticker.get_bars(start2)
    actual_bars = await ticker.get_bars(end1, start2)
    assert actual_bars.shape[0] == 60
    assert actual_bars.index[0] == pd.Timestamp(end1).ceil(settings.DEFAULT_TIMEFRAME)
    assert actual_bars.index[-1] == pd.Timestamp(start2).floor(
        settings.DEFAULT_TIMEFRAME
    )


async def test_get_bars_update_past(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    start1 = mdt.now() - timedelta(days=3)
    end1 = mdt.now() - timedelta(days=2)
    start2 = mdt.now() - timedelta(days=4)
    url = get_bars_url("AAPL", start1, end1)
    bars1 = generate_bars(start1, end1)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars1,
        },
    )
    result = await ticker.get_bars(start1, end1)
    url = get_bars_url("AAPL", start2, result.index[0])
    bars2 = generate_bars(start2, result.index[0])
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars2,
        },
    )
    url = get_bars_url("AAPL", result.index[-1])
    bars3 = generate_bars(result.index[-1])
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars3,
        },
    )
    await ticker.get_bars(start2)

    actual_bars = await ticker.get_bars(start2, start1)
    assert actual_bars.shape[0] == 24 * 60
    assert actual_bars.index[0] == pd.Timestamp(start2).ceil(settings.DEFAULT_TIMEFRAME)
    assert actual_bars.index[-1] == pd.Timestamp(start1).floor(
        settings.DEFAULT_TIMEFRAME
    )


async def test_get_bars_next_token(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() - timedelta(minutes=5)
    url = get_bars_url("AAPL", start)
    expected_bars = generate_bars(start)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": expected_bars[:2],
            "next_page_token": "token",
        },
    )
    httpx_mock.add_response(
        url=url + "&page_token=token",
        json={
            "bars": expected_bars[2:],
        },
    )
    expected_bars = [
        {BAR_FIELDS_TO_NAMES[key]: value for key, value in bar.items() if key != "t"}
        for bar in expected_bars
    ]
    bars = await ticker.get_bars(start)
    assert len(bars) == len(expected_bars)
    assert bars.to_dict("records") == expected_bars


async def test_get_bars_start_gt_end(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() + timedelta(days=1)
    end = mdt.now()
    bars = await ticker.get_bars(start, end)
    assert bars.shape[0] == 0


async def test_get_bars_resample(httpx_mock: HTTPXMock):
    ticker = AlpacaTicker("AAPL")
    now = mdt.now() - timedelta(minutes=4)
    url = get_bars_url("AAPL", now)
    expected_bars = generate_bars(now)
    httpx_mock.add_response(
        url=url,
        status_code=500,
    )
    httpx_mock.add_exception(
        url=url,
        exception=RequestError(500),
    )
    httpx_mock.add_response(
        url=url,
        json={
            "bars": expected_bars,
        },
    )
    expected_bars = [
        {BAR_FIELDS_TO_NAMES[key]: value for key, value in bar.items() if key != "t"}
        for bar in expected_bars
    ]
    bars = await ticker.get_bars(now, freq="2min")
    assert bars.index[-1] - bars.index[-2] == pd.Timedelta("2min")


async def test_subscribe_bars(start_ws_server: Callable):
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() - timedelta(minutes=5)
    expected_bars = [{"S": "AAPL", **bar} for bar in generate_bars(start)]
    received_cmds = await start_ws_server(
        [
            {"T": "success", "msg": "connected"},
            {"T": "success", "msg": "authenticated"},
            *expected_bars,
        ]
    )
    await ticker.subscribe_bars()
    await ticker.subscribe_bars()
    await asyncio.sleep(0.1)
    bars = await ticker.get_bars(start)
    assert bars.shape[0] == len(expected_bars)
    assert received_cmds.value == [
        {"action": "auth", "key": "key", "secret": "secret"},
        {"action": "subscribe", "bars": ["AAPL"]},
    ]
    ws = AlpacaWebSocket()
    ws._task.cancel()
    try:
        await ws._task
    except asyncio.CancelledError:
        pass
