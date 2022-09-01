import asyncio
import logging
import traceback
from abc import ABCMeta

import httpx
from httpx import RequestError

from . import settings

logger = logging.getLogger(__name__)


class MaxRetryError(Exception):
    pass


RETRIABLE_STATUS = [429, 500, 502, 503, 504]


async def retry_request(client: httpx.AsyncClient, *args, **kwargs) -> httpx.Response:
    """
    Auxiliary function to retry a request if the status code is in the list of retryable status codes.

    Args:
        client: (:obj:`httpx.AsyncClient`) The client to use for the request.
        *args: The arguments to pass to the request.
        **kwargs: The keyword arguments to pass to the request.

    Returns:
        :obj:`httpx.Response`: The response of the request.

    Raises:
        :obj:`MaxRetryError`: If the maximum number of retries is reached.
    """
    retry_count = 0
    response = None
    while retry_count <= settings.MAX_RETRIES:
        try:
            response = await client.request(*args, **kwargs)
            if (
                200 <= response.status_code < 300
            ) or response.status_code not in RETRIABLE_STATUS:
                return response
        except RequestError:
            if retry_count == settings.MAX_RETRIES:
                logger.error(f"RequestError: {traceback.format_exc()}")
                break
        await asyncio.sleep(0.1 * 2**retry_count)
        retry_count += 1
    if response is not None:
        logger.error(
            f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')}"
        )
    raise MaxRetryError("Max retries reached")


class SingletonMeta(ABCMeta):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
