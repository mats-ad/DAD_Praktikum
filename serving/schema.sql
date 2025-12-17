CREATE TABLE IF NOT EXISTS prices_daily (
  symbol TEXT NOT NULL,
  day DATE NOT NULL,
  price_day_last DOUBLE PRECISION,
  PRIMARY KEY(symbol, day)
);

CREATE TABLE IF NOT EXISTS social_daily (
  ticker TEXT NOT NULL,
  day DATE NOT NULL,
  mentions INTEGER NOT NULL,
  PRIMARY KEY(ticker, day)
);