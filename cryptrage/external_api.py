import time
from typing import Optional
from functools import wraps
import logging
import json
import asyncio

import krakenex
import gdax
import bitstamp.client as bclient
from requests.exceptions import RequestException
import websockets
from asyncpg.exceptions import InterfaceError

from cryptrage.exchanges import (create_kraken_tuple, Bitstamp, create_gdax_response,
                                 GDAX, Kraken, KRAKEN_MAPPING, localize_timestamp,
                                 create_bitstamp_response,
                                 create_gdax_ws_response,
                                 create_bitonic_ws_response)


logger = logging.getLogger(__name__)
WAIT_TIMEOUT = 320.0

def log_request_exception(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            return result
        except RequestException as e:
            logger.exception(f"Function {f} raised RequestException {e}")
            return
    return inner


@log_request_exception
def get_kraken(pair: str='XBTEUR') -> Optional[Kraken]:
    client = krakenex.API()

    before = time.time()
    response = client.query_public('Ticker', data={'pair': [pair]})

    after = time.time()
    if response.get('error'):
        logger.error(f"Krakanex got an error: {response.get('error')}")
        return

    # the API doesn't return the ts, so use a workaround
    ts = localize_timestamp((before + after) / 2)
    result_key = KRAKEN_MAPPING.get(pair)
    if response.get('result') and response.get('result').get(result_key):
        res = response.get('result').get(result_key)
        return create_kraken_tuple(timestamp=ts, response=res, pair=pair)
    else:
        logger.warning(f"Krakenex got no valid response: {response}")


@log_request_exception
def get_gdax(pair: str='BTC-EUR') -> Optional[GDAX]:
    client = gdax.PublicClient()
    response = client.get_product_ticker(product_id=pair)

    if response.get('message') or not response.get('trade_id'):
        logger.warning(f"GDAX got no valid response: {response}")
        return

    return create_gdax_response(response=response, pair=pair)


@log_request_exception
def get_bitstamp(base='BTC', quote='EUR') -> Optional[Bitstamp]:
    client = bclient.Public()
    response = client.ticker(base=base, quote=quote)

    if not response.get('timestamp'):
        logger.warning(f"Bitstamp got no valid response: {response}")
        return

    return create_bitstamp_response(response=response, base=base, quote=quote)


async def get_gdax_async(*, insert_function):
    """

    :param insert_function: A function whose first positional parameter is the NamedTuple to insert
    :param insert_kwargs: Kwargs for `insert_function`
    :return:
    """
    subscribe = json.dumps({
        "type": "subscribe",
        "product_ids": [
            "BTC-EUR"
        ],
        "channels": [
            {
                "name": "ticker",
                "product_ids": [
                    "BTC-EUR"
                ]
            }
        ]
    })
    gdax_ws_address = "wss://ws-feed.gdax.com"
    async with websockets.connect(gdax_ws_address) as websocket:
        await websocket.send(subscribe)
        logger.info(f"Connected to websocket {gdax_ws_address}")
        while True:
            message_str = await asyncio.wait_for(websocket.recv(), WAIT_TIMEOUT)
            logger.info(f"Received message {message_str}")
            message = json.loads(message_str)
            if message['type'] == 'ticker' and message.get('time'):
                response = create_gdax_ws_response(response=message, pair="BTC-EUR")
                await insert_function(response)
            else:
                logger.warning(f"Message {message_str} was not a ticker or "
                               f"did not have a time attribute")



async def get_bitonic_async(*, insert_function):
    """

    :param insert_function: A function whose first positional parameter is the NamedTuple to insert
    :param insert_kwargs: Kwargs for `insert_function`
    :return:
    """

    bitonic_address = "wss://api.bl3p.eu/1/BTCEUR/trades"
    async with websockets.connect(bitonic_address) as websocket:
        logger.info(f"Connected to websocket {bitonic_address}")
        while True:
            message_str = await asyncio.wait_for(websocket.recv(), WAIT_TIMEOUT)
            logger.info(f"Received message {message_str}")
            message = json.loads(message_str)
            response = create_bitonic_ws_response(response=message)
            await insert_function(response)