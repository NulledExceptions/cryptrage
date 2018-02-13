from typing import List, NamedTuple

from psycopg2.pool import AbstractConnectionPool
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import NamedTupleCursor

from cryptrage.database.utils import manage_pool_key, get_table_name



@manage_pool_key(factory=NamedTupleCursor)
def get_spreads(*, pool: AbstractConnectionPool=None, cursor: Cursor,
                table: str, schema: str=None,
                transaction_pct: float=0.25, **kwargs) -> List[NamedTuple]:
    # TODO the time interval of 10 minutes could become a variable
    table_name = sql.Identifier(get_table_name(schema=schema, table=table))
    transaction_ratio = sql.SQL(f"{transaction_pct / 100.}")
    statement = sql.SQL("""
    WITH sq AS (
    SELECT 
      LAST(ask_price, ts) ask, 
      LAST(bid_price, ts) bid, 
      LAST(ts, ts) ts,
      exchange
    FROM {0} 
    WHERE ts > now() - INTERVAL '10 minutes'
    GROUP BY exchange
    ),
    spreads AS (
    SELECT
      (q1.bid - q2.ask) AS spread,
      ROUND((q1.bid - q2.ask) / q2.ask * 100, 2) AS ask_pct,
      q2.ask,
      q1.exchange sell_to_exchange,
      q2.exchange buy_from_exchange,
      MD5(CONCAT(q1.exchange, q2.exchange)) exchanges_hash,
      q1.ts sell_to_ts,
      q2.ts buy_from_ts,
      EXTRACT(HOUR FROM now() AT TIME ZONE 'Europe/Amsterdam') current_hour
    FROM sq q1
    CROSS JOIN sq q2
    WHERE 
      q1.bid - q2.ask > (q1.bid * {1} + q2.ask * {1}))
    SELECT * FROM spreads
    WHERE ((ask_pct > 1 AND current_hour BETWEEN 7 AND 23) OR ask_pct > 2)
      AND (buy_from_ts - sell_to_ts BETWEEN INTERVAL '-5 seconds' AND INTERVAL '5 seconds');
    """).format(table_name, transaction_ratio)
    cursor.execute(statement)
    results = cursor.fetchall()
    return results
