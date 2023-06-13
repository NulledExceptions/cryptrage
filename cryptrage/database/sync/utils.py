import logging
from typing import Callable
from contextlib import contextmanager
from functools import wraps

from psycopg2.pool import AbstractConnectionPool
from psycopg2.extensions import cursor as Cursor


def get_table_name(*, table, schema=None):
    """Compute the qualified table name for a database.
    """
    if schema:
        table = f"{schema}.{table}"

    return table


@contextmanager
def get_cursor(pool: AbstractConnectionPool,
               key: str=None, factory: Cursor=None):
    try:
        with pool.getconn(key) as conn, conn.cursor(cursor_factory=factory) as cur:
            yield cur
    finally:
        pool.putconn(conn, key)


def manage_pool_key(key: str=None, factory: Cursor=None) -> Callable:
    """
    Allows to get easily get a cursor from a pool putting back the connection
    once it's done

    :param key: Argument for pool.getconn(key)
    :param factory: The type of cursor that should be returned
    :return: A decorated function

    :Example:

    >>> @manage_pool_key(factory=RealDictCursor)
    >>> def give_me_records(*, pool: AbstractConnectionPool, cursor: Cursor):
    >>>     cursor.execute("SELECT * FROM my_table")
    >>>     return cursor.fetchall()
    >>> # I need to pass a valid pool but I shouldn't pass a cursor!
    >>> results = give_me_records(pool)
    """
    def function(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            pool = kwargs.get('pool')
            if kwargs.get('cursor'):
                logging.warning("The passed cursor will be overwritten")
            with get_cursor(pool, key, factory) as cursor:
                result = function(*args, **{**kwargs, "cursor": cursor})
            return result
        return wrapper
    return function

# create a decorator without parameters
manage_pool = manage_pool_key()