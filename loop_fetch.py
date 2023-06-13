import os
from os import path
from typing import Callable
import threading
from time import sleep

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.tickers.external_api import get_kraken, get_bitstamp
from cryptrage.database.sync.insert import insert_ticker
from cryptrage.logging import setup_logging

config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'insert_db.yaml')
logger = setup_logging(config_path=config_path, name='insert')


def insert(get_function: Callable=None, pool: AbstractConnectionPool=None,
           **kwargs) -> None:
    t = None
    try:
        insert_kwargs = {"pool": pool, "table": "ticker", **kwargs}
        tick = get_function(**kwargs)
        insert_kwargs.update({"tick": tick})
        if tick:
            t = threading.Thread(target=insert_ticker,
                                 kwargs=insert_kwargs)
            t.start()
    except Exception as e:
        logger.exception(f"Called insert with {insert_kwargs}")
        raise e

    if t:
        t.join()  # in case the db is very slow, we wait for it


@pidfile(pidname='cryptrage', piddir=".")
def main() -> None:

    PGPASSWORD = os.environ.get("PGPASSWORD")

    if not PGPASSWORD:
        raise ValueError("PGPASSWORD was not a valid environment variable")
    dsn = f"host=localhost password={PGPASSWORD} dbname=timescale user=timescale"
    db_pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=dsn)
    while True:
        kraken = threading.Thread(target=insert, kwargs={"get_function": get_kraken,
                                                         "pool": db_pool})

        bitstamp = threading.Thread(target=insert, kwargs={"get_function": get_bitstamp,
                                                           "pool": db_pool})
        try:
            kraken.start()
            bitstamp.start()
        except Exception as e:
            logger.exception(f"Exception raised")
            db_pool.closeall()
            raise e
        sleep(1)



if __name__ == "__main__":
    main()
