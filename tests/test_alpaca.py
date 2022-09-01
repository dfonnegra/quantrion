import asyncio
from random import random
from typing import Callable, Optional
from urllib.parse import urlencode, urljoin

import pandas as pd
import pytz
from httpx import RequestError
from pytest_httpx import HTTPXMock

from quantrion import settings
from quantrion.asset.alpaca import BAR_FIELDS_TO_NAMES, AlpacaUSStock, AlpacaWebSocket
from quantrion.utils import MarketDatetime as mdt


def normalize_start_end(
    start: pd.Timestamp, end: Optional[pd.Timestamp] = None, freq: Optional[str] = None
):
    DTF = settings.DEFAULT_TIMEFRAME
    _freq = freq or DTF
    start = start.ceil(_freq)
    max_end = mdt.now().floor(_freq) - pd.Timedelta(DTF)
    if end is not None:
        end = end.floor(_freq)
        if _freq != DTF:
            end += pd.Timedelta(_freq) - pd.Timedelta(DTF)
        end = min(end, max_end)
    else:
        end = max_end
    return start.astimezone(pytz.UTC), end.astimezone(pytz.UTC)


def get_bars_url(
    symbol: str,
    start: pd.Timestamp,
    end: Optional[pd.Timestamp] = None,
    freq: Optional[str] = None,
):
    start, end = normalize_start_end(start, end, freq)
    params = {
        "timeframe": settings.DEFAULT_TIMEFRAME,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    url = urljoin(settings.ALPACA_DATA_URL, f"/v2/stocks/{symbol}/bars")
    return url + f"?{urlencode(params)}"


def generate_bars(
    start: pd.Timestamp, end: Optional[pd.Timestamp] = None, freq: Optional[str] = None
):
    start, end = normalize_start_end(start, end, freq)
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
    stock = AlpacaUSStock("AAPL")
    end = mdt.now()
    start = mdt.now() - pd.Timedelta("1h")
    url = get_bars_url("AAPL", start, end)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": [],
        },
    )
    bars = await stock.bars.get(start, end)
    assert bars.shape[0] == 0


async def test_get_bars_no_end(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    now = mdt.now() - pd.Timedelta("1h")
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
    bars = await stock.bars.get(now)
    assert len(bars) == len(expected_bars)
    assert bars.to_dict("records") == expected_bars


async def test_get_bars_update(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    start1 = mdt.now() - pd.Timedelta("4d")
    end1 = mdt.now() - pd.Timedelta("3d1h")
    start2 = mdt.now() - pd.Timedelta("3d")
    url = get_bars_url("AAPL", start1, end1)
    bars1 = generate_bars(start1, end1)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars1,
        },
    )
    result = await stock.bars.get(start1, end1)
    url = get_bars_url(
        "AAPL", result.index[-1] + pd.Timedelta(settings.DEFAULT_TIMEFRAME)
    )
    bars2 = generate_bars(result.index[-1] + pd.Timedelta(settings.DEFAULT_TIMEFRAME))
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars2,
        },
    )
    await stock.bars.get(start1, end1)
    await stock.bars.get(start2)
    actual_bars = await stock.bars.get(end1, start2)
    assert actual_bars.shape[0] == 60
    assert actual_bars.index[0] == end1.ceil(settings.DEFAULT_TIMEFRAME)
    assert actual_bars.index[-1] == start2.floor(settings.DEFAULT_TIMEFRAME)


async def test_get_bars_update_past(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    start1 = mdt.now() - pd.Timedelta("3d")
    end1 = mdt.now() - pd.Timedelta("2d")
    start2 = mdt.now() - pd.Timedelta("4d")
    url = get_bars_url("AAPL", start1, end1)
    bars1 = generate_bars(start1, end1)
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars1,
        },
    )
    result = await stock.bars.get(start1, end1)
    url = get_bars_url(
        "AAPL", start2, result.index[0] - pd.Timedelta(settings.DEFAULT_TIMEFRAME)
    )
    bars2 = generate_bars(
        start2, result.index[0] - pd.Timedelta(settings.DEFAULT_TIMEFRAME)
    )
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars2,
        },
    )
    url = get_bars_url(
        "AAPL", result.index[-1] + pd.Timedelta(settings.DEFAULT_TIMEFRAME)
    )
    bars3 = generate_bars(result.index[-1] + pd.Timedelta(settings.DEFAULT_TIMEFRAME))
    httpx_mock.add_response(
        url=url,
        json={
            "bars": bars3,
        },
    )
    await stock.bars.get(start2)

    actual_bars = await stock.bars.get(start2, start1)
    assert actual_bars.shape[0] == 24 * 60
    assert actual_bars.index[0] == start2.ceil(settings.DEFAULT_TIMEFRAME)
    assert actual_bars.index[-1] == start1.floor(settings.DEFAULT_TIMEFRAME)


async def test_get_bars_next_token(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    start = mdt.now() - pd.Timedelta("5min")
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
    bars = await stock.bars.get(start)
    assert len(bars) == len(expected_bars)
    assert bars.to_dict("records") == expected_bars


async def test_get_bars_start_gt_end(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    start = mdt.now() + pd.Timedelta("1d")
    end = mdt.now()
    bars = await stock.bars.get(start, end)
    assert bars.shape[0] == 0


async def test_get_bars_resample(httpx_mock: HTTPXMock):
    stock = AlpacaUSStock("AAPL")
    now = mdt.now() - pd.Timedelta("6min")
    url = get_bars_url("AAPL", now, freq="2min")
    expected_bars = generate_bars(now, freq="2min")
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
    bars = await stock.bars.get(now, freq="2min")
    assert bars.index[-1] - bars.index[-2] == pd.Timedelta("2min")


async def test_subscribe_bars(start_ws_server: Callable):
    stock = AlpacaUSStock("AAPL")
    start = mdt.now() - pd.Timedelta("5min")
    expected_bars = [{"S": "AAPL", **bar} for bar in generate_bars(start)]
    received_cmds = await start_ws_server(
        [
            {"T": "success", "msg": "connected"},
            {"T": "success", "msg": "authenticated"},
            *expected_bars,
        ]
    )
    await stock.bars.subscribe()
    await stock.bars.subscribe()
    await asyncio.sleep(0.2)
    bars = await stock.bars.get(start)
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
