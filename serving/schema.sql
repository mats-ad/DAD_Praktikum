-- serving/schema.sql
-- =========================================
-- GABI (DAD Praktikum) - Postgres Schema
-- =========================================

-- Optional: sauberes Reset bei "docker compose down -v"
-- (Wenn du persistente Volumes nutzt, lass DROPs ggf. weg)
-- DROP VIEW IF EXISTS joined_daily_signal;
-- DROP TABLE IF EXISTS joined_daily;
-- DROP TABLE IF EXISTS social_daily;
-- DROP TABLE IF EXISTS prices_daily;

-- -----------------------------------------
-- 1) Prices (Batch Daily)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS prices_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  price_day_last DOUBLE PRECISION,
  PRIMARY KEY(ticker, day)
);

-- -----------------------------------------
-- 2) Social (Batch Daily) + Sentiment
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS social_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  mentions INTEGER NOT NULL DEFAULT 0,
  avg_sentiment DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY(ticker, day)
);

-- -----------------------------------------
-- 3) Joined Daily (Grafana-friendly)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS joined_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  price_day_last DOUBLE PRECISION,
  mentions INTEGER NOT NULL DEFAULT 0,
  avg_sentiment DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (ticker, day)
);

-- Indexe für schnellere Zeitreihen-Queries in Grafana
CREATE INDEX IF NOT EXISTS idx_joined_daily_day ON joined_daily(day);
CREATE INDEX IF NOT EXISTS idx_joined_daily_ticker_day ON joined_daily(ticker, day);

-- -----------------------------------------
-- 4) Signal View (einfaches "Invest-Hinweis" Modell)
--    - daily_return: relative Tagesänderung
--    - invest_signal: 1 (bullish), -1 (bearish), 0 (neutral)
-- -----------------------------------------
CREATE OR REPLACE VIEW joined_daily_signal AS
WITH base AS (
  SELECT
    ticker,
    day,
    day::timestamp AS time, -- Grafana mag "time" als timestamp
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
    -- sehr simple Heuristik: "viel Aufmerksamkeit" + "positives Sentiment" + "nicht fallender Preis"
    WHEN mentions >= 10 AND avg_sentiment >= 0.20 AND daily_return IS NOT NULL AND daily_return >= 0 THEN 1

    -- "viel Aufmerksamkeit" + "negatives Sentiment" + "nicht steigender Preis"
    WHEN mentions >= 10 AND avg_sentiment <= -0.20 AND daily_return IS NOT NULL AND daily_return <= 0 THEN -1

    ELSE 0
  END AS invest_signal
FROM base;