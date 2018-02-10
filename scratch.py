import time
import krakenex
import decimal
import datetime
import pytz
from tzlocal import get_localzone
import gdax
import bitstamp.client as bclient

pair = 'XBTEUR'


def now():
    return datetime.datetime.now()



def localize_kraken(local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    return local_zone.localize(datetime.datetime.now())


def localize_gdax(dt: str, local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    utc_time = pytz.utc.localize(datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ"))
    return utc_time.astimezone(local_zone)


def localize_bitstamp(dt: str, local_zone: datetime.tzinfo=get_localzone()) -> datetime.datetime:
    return local_zone.localize(datetime.datetime.fromtimestamp(int(dt)))



# kraken
kraken_client = krakenex.API()

kraken_client.query_public('Ticker', data={'pair': [pair]})  # this doesn't return the ts, so use now()
print(localize_kraken())
# gdax
gdax_client = gdax.PublicClient()
gdax_response = gdax_client.get_product_ticker(product_id='BTC-EUR')  # time is in UTC
print(localize_gdax(gdax_response['time']))


# bitstamp
bistamp_client = bclient.Public()
bitstamp_response = bistamp_client.ticker(base='btc', quote='eur')
print(localize_bitstamp(bitstamp_response['timestamp']))


