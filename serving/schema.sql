CREATE TABLE IF NOT EXISTS prices_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  price_day_last DOUBLE PRECISION,
  PRIMARY KEY(ticker, day)
);

CREATE TABLE IF NOT EXISTS social_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  mentions INTEGER NOT NULL DEFAULT 0,
  avg_sentiment DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY(ticker, day)
);

CREATE TABLE IF NOT EXISTS joined_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  price_day_last DOUBLE PRECISION,
  mentions INTEGER NOT NULL DEFAULT 0,
  avg_sentiment DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (ticker, day)
);

CREATE INDEX IF NOT EXISTS idx_joined_daily_day ON joined_daily(day);
CREATE INDEX IF NOT EXISTS idx_joined_daily_ticker_day ON joined_daily(ticker, day);

CREATE OR REPLACE VIEW joined_daily_signal AS
WITH base AS (
  SELECT
    ticker,
    day,
    day::timestamp AS time, 
    price_day_last,
    mentions,
    avg_sentiment,
    (price_day_last - LAG(price_day_last) OVER (PARTITION BY ticker ORDER BY day))
      / NULLIF(LAG(price_day_last) OVER (PARTITION BY ticker ORDER BY day), 0) AS daily_return
  FROM joined_daily
)
SELECT
  ticker,
  day,
  time,
  price_day_last,
  mentions,
  avg_sentiment,
  daily_return,
  CASE
    WHEN mentions >= 10 AND avg_sentiment >= 0.20 AND daily_return IS NOT NULL AND daily_return >= 0 THEN 1

    WHEN mentions >= 10 AND avg_sentiment <= -0.20 AND daily_return IS NOT NULL AND daily_return <= 0 THEN -1

    ELSE 0
  END AS invest_signal
FROM base;