import asyncio
from datetime import timedelta

from quantrion.ticker.alpaca import AlpacaTicker
from quantrion.utils import MarketDatetime as mdt


async def _run():
    ticker = AlpacaTicker("AAPL")
    start = mdt.now() - timedelta(days=1)
    bars = await ticker.get_bars(start)
    await ticker.subscribe_bars()
    while True:
        await asyncio.sleep(1)


def run():
    asyncio.run(_run())
