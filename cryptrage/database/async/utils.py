import time

import asyncpg
from asyncpg.pool import Pool

from cryptrage.database.async.insert import logger


async def create_pool(*, dsn: str, min_conn: int=2, max_conn: int=10, **kwargs) -> Pool:
    pool = await asyncpg.create_pool(dsn=dsn,
                                     min_size=min_conn,
                                     max_size=max_conn,
                                     **kwargs)
    logger.info("Pool created")
    return pool


def get_pool(dsn, loop):
    attempts = 20
    sleep_between_attempts = 3
    for _ in range(attempts):
        try:
            pool = loop.run_until_complete(create_pool(dsn=dsn))
        except Exception as e:  # database is not ready yet
            logger.exception(e)
            time.sleep(sleep_between_attempts)
        else:
            return pool
    raise Exception(f"Could not connect to database using {dns} after "
                    f"{attempts * sleep_between_attempts} seconds")