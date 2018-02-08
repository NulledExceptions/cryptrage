CREATE TABLE IF NOT EXISTS ticker
(
  ts                TIMESTAMPTZ NOT NULL,
  exchange          VARCHAR(10)    NOT NULL,
  base              CHAR(3)     NOT NULL,
  quote             CHAR(3)     NOT NULL,
  last_trade_price  DECIMAL(10, 2),
  last_trade_volume DECIMAL(16, 8),
  ask_price         DECIMAL(10, 2),
  ask_wlv           INTEGER,
  ask_volume        DECIMAL(16, 8),
  bid_price         DECIMAL(10, 2),
  bid_wlv           INTEGER,
  bid_volume        DECIMAL(16, 8),
  volume_today      DECIMAL(16, 8),
  volume_24h        DECIMAL(16, 8),
  vwap_today        DECIMAL(16, 8),
  vwap_24h          DECIMAL(16, 8),
  trades_today      INTEGER,
  trades_24h        INTEGER,
  low_today         DECIMAL(10, 2),
  low_24h           DECIMAL(10, 2),
  high_today        DECIMAL(10, 2),
  high_24h          DECIMAL(10, 2),
  opening           DECIMAL(10, 2),
  trade_id          INTEGER
);
COMMENT ON COLUMN ticker.base IS 'This is usually the cryptocurrency';
COMMENT ON COLUMN ticker.quote IS 'This is usually the currency';
COMMENT ON COLUMN ticker.trade_id IS 'This only applies to GDAX';
CREATE INDEX ON ticker (exchange, ts DESC);

SELECT create_hypertable('ticker', 'ts', chunk_time_interval => interval '1 day');

