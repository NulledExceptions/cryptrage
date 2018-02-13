import os
from os import path
from time import sleep
from typing import Callable
from multiprocessing.dummy import Pool as ThreadPool
import threading

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.external_api import get_kraken, get_bitstamp
from cryptrage.database.insert import insert_ticker
from cryptrage.logging import setup_logging

config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'insert_db.yaml')
logger = setup_logging(config_path=config_path, name='insert')


def insert(get_function: Callable=None, pool: AbstractConnectionPool=None,
           sleep_for: int=1, **kwargs) -> None:
    while True:
        t = None
        try:
            tick = get_function(**kwargs)
            if tick:
                t = threading.Thread(target=insert_ticker,
                                     kwargs={"pool": pool, "tick": tick, "table": "ticker", **kwargs})
                t.start()
        except Exception as e:
            logger.exception(f"Called insert with {kwargs}", **kwargs)
        sleep(sleep_for)
        if t:
            t.join()  # in case the db is very slow, we wait for it


@pidfile(pidname='cryptrage', piddir=".")
def main() -> None:

    PGPASSWORD = os.environ.get("PGPASSWORD")

    if not PGPASSWORD:
        raise ValueError("PGPASSWORD was not a valid environment variable")
    dsn = f"host=localhost password={PGPASSWORD} dbname=timescale user=timescale"
    db_pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=dsn)
    thread_pool = ThreadPool(3)
    thread_pool.map(lambda get_function: insert(get_function=get_function,
                                                pool=db_pool,
                                                sleep_for=1),
                    [get_kraken, get_bitstamp])


if __name__ == "__main__":
    main()
