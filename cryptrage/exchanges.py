from collections import namedtuple
import datetime
from typing import Dict, Union
from decimal import Decimal


import pytz
from tzlocal import get_localzone


KRAKEN_MAPPING = {'XBTEUR': 'XXBTZEUR'}
GDAP_MAPPING = {'BTC': 'XBT'}
BITSTAMP_MAPPING = {'BTC': 'XBT'}
TS = Union[str, int]


Kraken = namedtuple('Kraken', 'ts exchange base quote last_trade_price last_trade_volume '
                              'ask_price ask_wlv ask_volume bid_price bid_wlv bid_volume '
                              'volume_today volume_24h vwap_today vwap_24h trades_today trades_24h '
                              'low_today low_24h high_today high_24h opening')

Bitstamp = namedtuple('Bitstamp', 'ts exchange base quote last_trade_price high_24h low_24h '
                                  'vwap_24h volume_24h bid_price ask_price opening')

GDAX = namedtuple('GDAX', 'ts exchange base quote trade_id last_trade_price last_trade_volume '
                          'bid_price ask_price volume_24h')

GDAX_WS = namedtuple('GDAX_WS', 'ts exchange base quote trade_id last_trade_price '
                                'last_trade_volume volume_24h low_24h high_24h '
                                'ask_price bid_price')

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


def create_kraken_tuple(*, response: Dict, timestamp: datetime.datetime, pair: str) -> Kraken:
    return Kraken(ts=timestamp, exchange='kraken', base=pair[:3], quote=pair[3:],
                  last_trade_price=Decimal(response.get('c')[0]),
                  last_trade_volume=Decimal(response.get('c')[1]),
                  ask_price=Decimal(response.get('a')[0]),
                  ask_wlv=Decimal(response.get('a')[1]),
                  ask_volume=Decimal(response.get('a')[2]),
                  bid_price=Decimal(response.get('b')[0]),
                  bid_wlv=Decimal(response.get('b')[1]),
                  bid_volume=Decimal(response.get('b')[2]),
                  volume_today=Decimal(response.get('v')[0]),
                  volume_24h=Decimal(response.get('v')[1]),
                  vwap_today=Decimal(response.get('p')[0]),
                  vwap_24h=Decimal(response.get('p')[1]),
                  trades_today=int(response.get('t')[0]),
                  trades_24h=int(response.get('t')[1]),
                  low_today=Decimal(response.get('l')[0]),
                  low_24h=Decimal(response.get('l')[1]),
                  high_today=Decimal(response.get('h')[0]),
                  high_24h=Decimal(response.get('h')[1]),
                  opening=Decimal(response.get('o')))


def create_gdax_response(*, response: Dict, pair: str) -> GDAX:
    return GDAX(ts=localize_gdax(response.get('time')),
                exchange='GDAX', base=GDAP_MAPPING.get(pair[:3]),
                quote=pair[4:], trade_id=int(response.get('trade_id')),
                last_trade_price=Decimal(response.get('price')),
                last_trade_volume=Decimal(response.get('size')),
                bid_price=Decimal(response.get('bid')),
                ask_price=Decimal(response.get('ask')),
                volume_24h=Decimal(response.get('volume')))


def create_gdax_ws_response(*, response: Dict, pair: str) -> GDAX_WS:
    return GDAX_WS(ts=localize_gdax(response.get('time')),
                   exchange='GDAX', base=GDAP_MAPPING.get(pair[:3]),
                   quote=pair[4:], trade_id=int(response.get('trade_id')),
                   last_trade_price=Decimal(response.get('price')),
                   last_trade_volume=Decimal(response.get('last_size')),
                   volume_24h=Decimal(response.get('volume_24h')),
                   low_24h=Decimal(response.get('low_24h')),
                   high_24h=Decimal(response.get('high_24h')),
                   bid_price=Decimal(response.get('best_bid')),
                   ask_price=Decimal(response.get('best_ask')))


def create_bitstamp_response(*, response: Dict, base: str, quote: str) -> Bitstamp:
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
