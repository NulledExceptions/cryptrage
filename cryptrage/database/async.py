from typing import NamedTuple
import logging


import asyncpg
from asyncpg.pool import Pool  # type-hinting

logger = logging.getLogger(__name__)

async def create_pool(*, dsn: str, min_conn: int=10, max_conn: int=10, **kwargs) -> Pool:
    pool = await asyncpg.create_pool(dsn=dsn,
                                     min_size=min_conn,
                                     max_size=max_conn,
                                     **kwargs)
    logger.info("Pool created")
    return pool


async def insert_ticker(tick: NamedTuple, *, pool: Pool, schema: str=None, table: str) -> None:
    """Handle inserting ticks"""
    fields = tick._fields
    placeholders = ['${}'.format(i) for i, _ in enumerate(fields, 1)]
    query = 'INSERT INTO {} ({}) VALUES ({})'.format(
        table, ', '.join(fields), ', '.join(placeholders))
    async with pool.acquire() as connection:
        async with connection.transaction():
             await connection.execute(query, *tick)