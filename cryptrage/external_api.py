from decimal import Decimal
import time
import datetime
from typing import Union, Optional
from collections import namedtuple
from functools import wraps

import pytz
from tzlocal import get_localzone
import krakenex
import gdax
import bitstamp.client as bclient
from requests.exceptions import RequestException

from cryptrage.logging import log_api_error, log_missing_response, log_exception

utcnow = datetime.datetime.utcnow

TS = Union[str, int]

KRAKEN_MAPPING = {'XBTEUR': 'XXBTZEUR'}
GDAP_MAPPING = {'BTC': 'XBT'}
BITSTAMP_MAPPING = {'BTC': 'XBT'}

Kraken = namedtuple('Kraken', 'ts exchange base quote last_trade_price last_trade_volume '
                              'ask_price ask_wlv ask_volume bid_price bid_wlv bid_volume '
                              'volume_today volume_24h vwap_today vwap_24h trades_today trades_24h '
                              'low_today low_24h high_today high_24h opening')

Bitstamp = namedtuple('Bitstamp', 'ts exchange base quote last_trade_price high_24h low_24h '
                                  'vwap_24h volume_24h bid_price ask_price opening')

GDAX = namedtuple('GDAX', 'ts exchange base quote trade_id last_trade_price last_trade_volume '
                          'bid_price ask_price volume_24h')

def now():
    return datetime.datetime.now()


def localize_kraken(local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    return local_zone.localize(datetime.datetime.now())


def localize_gdax(dt: str, local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    utc_time = pytz.utc.localize(datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ"))
    return utc_time.astimezone(local_zone)


def localize_timestamp(dt: TS, local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    """
    Convert a unix timestamp (UTC, by definitation) to a timestamp aware datetime

    :param dt: A string or integer representing unix timestamp
    :param local_zone: The timezone we want to localize the datetime in. Default to machine timezone
    :return: A timezone aware datetime object
    """
    return datetime.datetime.fromtimestamp(int(dt), tz=local_zone)


def log_request_exception(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            return result
        except RequestException as e:
            log_exception(e)
            return
    return inner


@log_request_exception
def get_kraken(pair: str='XBTEUR') -> Optional[Kraken]:
    client = krakenex.API()

    before = time.time()
    response = client.query_public('Ticker', data={'pair': [pair]})

    after = time.time()
    if response.get('error'):
        log_api_error(response.get('error'))
        return

    # the API doesn't return the ts, so use a workaround
    ts = localize_timestamp((before + after) / 2)
    result_key = KRAKEN_MAPPING.get(pair)
    if response.get('result') and response.get('result').get(result_key):
        res = response.get('result').get(result_key)
        return Kraken(ts=ts, exchange='kraken', base=pair[:3], quote=pair[3:],
                      last_trade_price=Decimal(res.get('c')[0]),
                      last_trade_volume=Decimal(res.get('c')[1]),
                      ask_price=Decimal(res.get('a')[0]),
                      ask_wlv=Decimal(res.get('a')[1]),
                      ask_volume=Decimal(res.get('a')[2]),
                      bid_price=Decimal(res.get('b')[0]),
                      bid_wlv=Decimal(res.get('b')[1]),
                      bid_volume=Decimal(res.get('b')[2]),
                      volume_today=Decimal(res.get('v')[0]),
                      volume_24h=Decimal(res.get('v')[1]),
                      vwap_today=Decimal(res.get('p')[0]),
                      vwap_24h=Decimal(res.get('p')[1]),
                      trades_today=int(res.get('t')[0]),
                      trades_24h=int(res.get('t')[1]),
                      low_today=Decimal(res.get('l')[0]),
                      low_24h=Decimal(res.get('l')[1]),
                      high_today=Decimal(res.get('h')[0]),
                      high_24h=Decimal(res.get('h')[1]),
                      opening=Decimal(res.get('o')))
    else:
        log_missing_response(response=response)


@log_request_exception
def get_gdax(pair: str='BTC-EUR') -> Optional[GDAX]:
    client = gdax.PublicClient()
    response = client.get_product_ticker(product_id=pair)

    if response.get('message') or not response.get('trade_id'):
        log_missing_response(response=response)
        return

    return GDAX(ts=localize_gdax(response.get('time')),
                exchange='GDAX', base=GDAP_MAPPING.get(pair[:3]),
                quote=pair[4:], trade_id=int(response.get('trade_id')),
                last_trade_price=Decimal(response.get('price')),
                last_trade_volume=Decimal(response.get('size')),
                bid_price=Decimal(response.get('bid')),
                ask_price=Decimal(response.get('ask')),
                volume_24h=Decimal(response.get('volume')))


@log_request_exception
def get_bitstamp(base='BTC', quote='EUR') -> Optional[Bitstamp]:
    client = bclient.Public()
    response = client.ticker(base=base, quote=quote)

    if not response.get('timestamp'):
        log_missing_response(response=response)
        return

    return Bitstamp(ts=localize_timestamp(response.get('timestamp')),
                    exchange='Bitstamp', base=BITSTAMP_MAPPING.get(base),
                    quote=quote,
                    high_24h=Decimal(response.get('high')),
                    last_trade_price=Decimal(response.get('last')),
                    bid_price=Decimal(response.get('bid')),
                    ask_price=Decimal(response.get('ask')),
                    vwap_24h=Decimal(response.get('vwap')),
                    volume_24h=Decimal(response.get('volume')),
                    low_24h=Decimal(response.get('low')),
                    opening=Decimal(response.get('open')))