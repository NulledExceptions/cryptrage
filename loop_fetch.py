import os
from time import sleep
from typing import Callable
from multiprocessing.dummy import Pool as ThreadPool
import threading

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.external_api import get_kraken, get_gdax, get_bitstamp
from cryptrage.database.insert import insert_ticker
from cryptrage.logging import log_exception



def insert(get_function: Callable=None, pool: AbstractConnectionPool=None,
           sleep_for: int=1, **kwargs) -> None:
    while True:
        t = None
        try:
            tick = get_function(**kwargs)
            if tick:
                t = threading.Thread(target=insert_ticker,
                                     kwargs={"pool": pool, "tick": tick, "table": "ticker"})
                t.start()
        except Exception as e:
            log_exception(exception=e)  # TODO: think what should we do here
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
    thread_pool.map(lambda get_function: insert(get_function=get_function, pool=db_pool, sleep_for=5),
                    [get_kraken, get_gdax, get_bitstamp])


if __name__ == "__main__":
    main()