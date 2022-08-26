import asyncio
import os
from unittest.mock import patch

import pytest

os.environ["ALPACA_API_KEY_ID"] = "key"
os.environ["ALPACA_API_KEY_SECRET"] = "secret"

from quantrion.ticker.ticker import TickerMeta
from quantrion.ticker.alpaca import AlpacaTicker
from quantrion.utils import SingletonMeta


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    debug_patcher = patch("quantrion.settings.DEBUG", False)
    SingletonMeta._instances = {}
    for class_ in TickerMeta._classes:
        class_._instances = None
    debug_patcher.start()
    yield
    debug_patcher.stop()
