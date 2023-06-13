from typing import NamedTuple

from psycopg2.pool import AbstractConnectionPool
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor


from cryptrage.database.sync.utils import manage_pool, get_table_name


@manage_pool
def insert_ticker(*, pool: AbstractConnectionPool=None, tick: NamedTuple, cursor: Cursor,
                  table: str, schema: str=None, **kwargs):

    table_name = sql.Identifier(get_table_name(schema=schema, table=table))
    fields = sql.SQL(', ').join(sql.Identifier(n) for n in tick._fields)

    statement = sql.SQL("INSERT INTO {0} ({1}) VALUES %s").format(table_name, fields)
    cursor.execute(statement, (tick, ))