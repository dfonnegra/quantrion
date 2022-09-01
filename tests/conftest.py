import asyncio
import json
import os
from collections import namedtuple
from unittest.mock import patch

import pytest
import websockets

os.environ["ALPACA_API_KEY_ID"] = "key"
os.environ["ALPACA_API_KEY_SECRET"] = "secret"
os.environ["ALPACA_STREAMING_URL"] = "ws://localhost:44444"

from quantrion.asset.base import AssetMeta
from quantrion.utils import SingletonMeta


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def run_before_and_after_tests():
    debug_patcher = patch("quantrion.settings.DEBUG", False)
    SingletonMeta._instances = {}
    for class_ in AssetMeta._classes:
        class_._instances = None
    debug_patcher.start()
    yield
    debug_patcher.stop()


@pytest.fixture(scope="session")
async def start_ws_server():
    server = None
    symbol_data = namedtuple("symbol_data", ["value"])
    received_cmds = namedtuple("received_cmds", ["value"])
    ws_protocol = None

    async def start(sd):
        nonlocal server
        symbol_data.value = sd
        received_cmds.value = []
        if server is not None:
            return received_cmds

        async def handler(ws_p):
            nonlocal ws_protocol
            ws_protocol = ws_p
            symbols = {data["S"] for data in symbol_data.value if "S" in data}
            n_recv = len(symbols) + 1
            try:
                for _ in range(n_recv):
                    line: str = await ws_protocol.recv()
                    received_cmds.value.append(json.loads(line))
                for data in symbol_data.value:
                    await ws_protocol.send(json.dumps([data]))
                    await asyncio.sleep(0.01)
                while True:
                    await ws_protocol.recv()
            except (
                websockets.exceptions.ConnectionClosedOK,
                websockets.exceptions.ConnectionClosedError,
            ):
                return

        server = await websockets.serve(handler, host="", port=44444)
        return received_cmds

    yield start
    if server is not None:
        server.close()
        await server.wait_closed()
