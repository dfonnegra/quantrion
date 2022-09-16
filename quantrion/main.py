import asyncio
import logging
import logging.config
from datetime import timedelta

import pandas as pd
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
    now = pd.Timestamp.utcnow()
    start = stock.localize(now) - timedelta(days=2)
    stock_bars = await stock.bars.get(start, freq="2min")
    start = crypto.localize(now) - timedelta(days=2)
    crypto_bars = await crypto.bars.get(start, freq="2min")
    print(stock.symbol)
    print(stock_bars.tail())
    print(crypto.symbol)
    print(crypto_bars.tail())
    await stock.bars.subscribe()
    await crypto.bars.subscribe()
    while True:
        last_stock_bar, last_crypto_bar = await asyncio.gather(
            stock.bars.wait_for_next(freq="2min"),
            crypto.bars.wait_for_next(freq="2min"),
        )
        print(pd.Timestamp.now())
        print(stock.symbol)
        print(last_stock_bar.to_frame().T)
        print(crypto.symbol)
        print(last_crypto_bar.to_frame().T)


def run():
    asyncio.run(_run())
