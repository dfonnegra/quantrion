import asyncio
import logging
import logging.config
from datetime import timedelta

import yaml

from quantrion.ticker.alpaca import AlpacaTicker
from quantrion.utils import MarketDatetime as mdt

with open("logging.yaml", "r") as log_config_file:
    config = yaml.load(log_config_file, Loader=yaml.FullLoader)

logger = logging.getLogger()
logging.config.dictConfig(config)
logging.getLogger("websockets").addHandler(logging.NullHandler())
logging.getLogger("websockets").propagate = False
logging.getLogger("httpx").addHandler(logging.NullHandler())
logging.getLogger("httpx").propagate = False
logger.setLevel(logging.DEBUG)


async def _run():
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() - timedelta(minutes=30)
    bars = await ticker.bars.get(start, freq="5min")
    print(bars)
    await ticker.bars.subscribe()
    while True:
        new = await ticker.bars.wait_for_next(freq="5min")
        print(new.to_frame().T)


def run():
    asyncio.run(_run())
