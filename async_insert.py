import asyncio
import sys
import os
from os import path
from concurrent.futures import FIRST_EXCEPTION

from pid import PidFile

from cryptrage.tickers.external_api import get_gdax_async, get_bitonic_async
from cryptrage.database.async.insert import insert_ticker
from cryptrage.logging import setup_logging
from cryptrage.database.async.utils import get_pool

config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'insert_async.yaml')
logger = setup_logging(config_path=config_path, name='insert_async')


async def run_tasks(futures):
    # one of the coroutines has raised, they should run indefinitely
    done, _ = await asyncio.wait(futures, return_when=FIRST_EXCEPTION)
    # I do the try/except dance to cleanly log the exception
    # TODO once the logs will say from the the exceptions originate, move the try/except there
    # TODO and there log and raise
    try:
        for future in done:
            future.result()
    except:
        logger.exception("One coroutine failed, will restart now")


def main(exchange) -> None:
    PGPASSWORD = os.environ.get("PGPASSWORD")
    PGHOST = os.environ.get("PGHOST", 'localhost')
    if not PGPASSWORD:
        raise ValueError("PGPASSWORD was not a valid environment variable")

    dsn = f"postgres://timescale:{PGPASSWORD}@{PGHOST}/timescale"

    if exchange.lower() == 'gdax':
        get_async = get_gdax_async
    elif exchange.lower() == 'bitonic':
        get_async = get_bitonic_async
    else:
        message = f'Unrecognized exchange f{exchange}'
        logger.error(message)
        raise ValueError(message)

    while True:
        try:
            pool = get_pool(dsn, asyncio.get_event_loop())
        except Exception as e:
            logger.exception("Could not create pool")

        insert_ticker_async = lambda ticker: insert_ticker(ticker, pool=pool, table='ticker')

        asyncio.get_event_loop().run_until_complete(run_tasks([
            get_async(insert_function=insert_ticker_async)]))

        try:
            asyncio.get_event_loop().run_until_complete(pool.close())
        except Exception as e:
            logger.exception("Could not close pool")


if __name__ == '__main__':
    exchange = sys.argv[1]
    pid_file = f'insert_async_{exchange}'

    for handler in logger.parent.handlers:
        if handler.name == 'insert_async':
            handler.baseFilename += f".{exchange}"

    with PidFile(pid_file, piddir='.'):
        main(exchange)