-- serving/investment_signal.sql
-- Ziel: IMMER erfolgreich ausführbar (egal ob vorher View oder Materialized View existiert)

DROP MATERIALIZED VIEW IF EXISTS public.investment_signal_daily;
DROP VIEW IF EXISTS public.investment_signal_daily;

CREATE VIEW public.investment_signal_daily AS
WITH base AS (
  SELECT
    ticker,
    day,
    price_day_last,
    avg_sentiment,
    mentions::bigint AS mentions,
    (avg_sentiment - LAG(avg_sentiment) OVER (PARTITION BY ticker ORDER BY day)) AS sentiment_delta,
    (mentions - LAG(mentions) OVER (PARTITION BY ticker ORDER BY day))::double precision AS mentions_delta,
    (price_day_last - LAG(price_day_last) OVER (PARTITION BY ticker ORDER BY day)) AS price_delta
  FROM public.joined_daily
),
scored AS (
  SELECT
    *,
    CASE
      WHEN sentiment_delta > 0.20 AND mentions_delta > 50 AND price_delta <= 0 THEN 'BUY'
      WHEN sentiment_delta < -0.20 AND mentions_delta > 50 AND price_delta < 0 THEN 'SELL'
      ELSE 'HOLD'
    END AS signal
  FROM base
)
SELECT * FROM scored;