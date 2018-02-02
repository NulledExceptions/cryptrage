import os
from time import sleep
from typing import Callable
from multiprocessing.dummy import Pool as ThreadPool
import threading


from cryptrage.external_api import get_kraken, get_gdax, get_bitstamp
from cryptrage.database.insert import insert_ticker
from cryptrage.logging import log_exception
from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool




def insert(get_function: Callable=None, pool: AbstractConnectionPool=None,
           sleep_for: int=1, **kwargs):
    while True:
        t = None
        try:
            tick = get_function(**kwargs)
            t = threading.Thread(target=insert_ticker,
                                 kwargs={"pool": pool, "tick": tick, "table": "ticker"})
            t.start()
        except Exception as e:
            log_exception(exception=e)  # TODO: think what should we do here
        sleep(sleep_for)
        if t:
            t.join()  # in case the db is very slow, we wait for it



def main():
    PGPASSWORD = os.environ.get("PGPASSWORD")

    if not PGPASSWORD:
        raise ValueError("PGPASSWORD was not a valid environment variable")
    dsn = f"host=localhost password={PGPASSWORD} dbname=timescale user=timescale"
    db_pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=dsn)
    thread_pool = ThreadPool(3)
    thread_pool.map(lambda get_function: insert(get_function=get_function, pool=db_pool, sleep_for=20),
                    [get_kraken, get_gdax, get_bitstamp])