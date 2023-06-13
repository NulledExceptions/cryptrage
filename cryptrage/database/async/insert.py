from typing import NamedTuple
import logging

from asyncpg.pool import Pool  # type-hinting

logger = logging.getLogger(__name__)


async def insert_ticker(tick: NamedTuple, *, pool: Pool, schema: str=None, table: str) -> None:
    """Handle inserting ticks"""
    fields = tick._fields
    placeholders = ['${}'.format(i) for i, _ in enumerate(fields, 1)]
    query = 'INSERT INTO {} ({}) VALUES ({})'.format(
        table, ', '.join(fields), ', '.join(placeholders))
    async with pool.acquire() as connection:
        async with connection.transaction():
             await connection.execute(query, *tick)