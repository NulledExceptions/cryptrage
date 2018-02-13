import asyncio
import os
from os import path

from pid.decorator import pidfile

from cryptrage.external_api import get_gdax_async
from cryptrage.database.async import create_pool, insert_ticker
from cryptrage.logging import setup_logging

config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'insert_async.yaml')
logger = setup_logging(config_path=config_path, name='insert_async')


@pidfile(pidname='async_insert', piddir='.')
def main() -> None:
    PGPASSWORD = os.environ.get("PGPASSWORD")

    if not PGPASSWORD:
        raise ValueError("PGPASSWORD was not a valid environment variable")

    dsn = f"postgres://timescale:{PGPASSWORD}@localhost/timescale"
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(create_pool(dsn=dsn))

    asyncio.get_event_loop().run_until_complete(asyncio.wait([
        get_gdax_async(insert_function=insert_ticker,
                       insert_kwargs={"pool": pool,
                                      "table": "ticker"})
    ]))


if __name__ == '__main__':
    main()