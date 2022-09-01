import asyncio
import logging
import logging.config
from datetime import timedelta

import yaml

from quantrion.asset.alpaca import AlpacaCrypto, AlpacaUSStock

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
    stock = AlpacaUSStock("AAPL")
    crypto = AlpacaCrypto("BTC/USD")
    start = stock.dt.now() - timedelta(minutes=30)
    stock_bars = await stock.bars.get(start, freq="2min")
    start = crypto.dt.now() - timedelta(minutes=30)
    crypto_bars = await crypto.bars.get(start, freq="2min")
    print(stock.symbol)
    print(stock_bars)
    print(crypto.symbol)
    print(crypto_bars)
    await stock.bars.subscribe()
    await crypto.bars.subscribe()
    while True:
        # new = await stock.bars.wait_for_next(freq="5min")
        new = await crypto.bars.wait_for_next(freq="2min")
        print(new.to_frame().T)


def run():
    asyncio.run(_run())
