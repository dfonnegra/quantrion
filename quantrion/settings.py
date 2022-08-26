import os
from locale import T_FMT

from dotenv import load_dotenv

load_dotenv()

# General settings
DEBUG = True
DEFAULT_TIMEFRAME = "1min"
DEFAULT_POLL_INTERVAL = 0.001
MAX_RETRIES = 3

# Alpaca settings
ALPACA_API_KEY_ID = os.environ["ALPACA_API_KEY_ID"]
ALPACA_API_KEY_SECRET = os.environ["ALPACA_API_KEY_SECRET"]
ALPACA_DATA_URL = os.environ["ALPACA_DATA_URL"]
ALPACA_STREAMING_URL = os.environ["ALPACA_STREAMING_URL"]