import asyncio
import logging
import logging.config
from datetime import timedelta

import pandas as pd
import yaml

from quantrion.asset.alpaca import AlpacaCrypto, AlpacaUSStock
from quantrion.data.file import CSVAssetListProvider
from quantrion.strategy.supertrend import SupertrendStrategy
from quantrion.trading.alpaca import AlpacaTradingProvider, AlpacaTradingWebSocket
from quantrion.trading.schemas import OrderType, Side, TimeInForce

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
    tl_provider = CSVAssetListProvider("files/good_cryptos.csv", AlpacaCrypto)
    strategy = SupertrendStrategy(
        tl_provider,
        freq="2min",
        short_n=15,
        long_n=40,
        short_k=1.3,
        long_k=3.6,
        risk_multiplier=1.5,
        win_to_loss_ratio=2,
    )
    await strategy.run()


def run():
    asyncio.run(_run())
