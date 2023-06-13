from typing import List, NamedTuple

from psycopg2.pool import AbstractConnectionPool
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import NamedTupleCursor

from cryptrage.database.sync.utils import manage_pool_key, get_table_name



@manage_pool_key(factory=NamedTupleCursor)
def get_spreads(*, pool: AbstractConnectionPool=None, cursor: Cursor,
                table: str, schema: str=None,
                transaction_pct: float=0.25, **kwargs) -> List[NamedTuple]:
    # TODO the time interval of 10 minutes could become a variable
    table_name = sql.Identifier(get_table_name(schema=schema, table=table))
    transaction_ratio = sql.SQL(f"{transaction_pct / 100.}")
    statement = sql.SQL("""
    WITH latest_spreads AS (
    SELECT 
      LAST(ask_price, ts) ask, 
      LAST(bid_price, ts) bid, 
      LAST(ts, ts) ts,
      exchange
    FROM {0} 
    WHERE ts > now() - INTERVAL '1 minute'
    GROUP BY exchange
    ),
    spreads AS (
    SELECT
      (sell_to.bid - buy_from.ask) AS spread,
      ROUND((sell_to.bid - buy_from.ask) / buy_from.ask * 100, 2) AS ask_pct,
      buy_from.ask,
      sell_to.exchange sell_to_exchange,
      buy_from.exchange buy_from_exchange,
      MD5(CONCAT(sell_to.exchange, buy_from.exchange)) exchanges_hash,
      sell_to.ts sell_to_ts,
      buy_from.ts buy_from_ts,
      EXTRACT(HOUR FROM now() AT TIME ZONE 'Europe/Amsterdam') current_hour
    FROM latest_spreads sell_to
    CROSS JOIN latest_spreads buy_from
    WHERE 
      sell_to.bid - buy_from.ask > (sell_to.bid * {1} + buy_from.ask * {1}))
    SELECT * FROM spreads
    WHERE ((ask_pct > 1 AND current_hour BETWEEN 7 AND 23) OR ask_pct > 2)
      AND (buy_from_ts - sell_to_ts BETWEEN INTERVAL '-5 seconds' AND INTERVAL '5 seconds');
    """).format(table_name, transaction_ratio)
    cursor.execute(statement)
    results = cursor.fetchall()
    return results
