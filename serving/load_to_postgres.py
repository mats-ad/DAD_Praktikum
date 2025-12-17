import os, glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "gabi")
PG_USER = os.getenv("PG_USER", "gabi")
PG_PASSWORD = os.getenv("PG_PASSWORD", "gabi_pw")

PRICES_DAILY_PATH = "data/lake/prices/batch_daily/*.parquet"
SOCIAL_DAILY_PATH = "data/lake/social/reddit/batch_daily/*.parquet"

def conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)

def load_prices_daily(cur):
    files = sorted(glob.glob(PRICES_DAILY_PATH))
    if not files:
        print("No prices daily parquet.")
        return
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["day"] = pd.to_datetime(df["day"]).dt.date
    rows = list(df[["symbol","day","price_day_last"]].itertuples(index=False, name=None))
    execute_values(cur, """
        INSERT INTO prices_daily(symbol, day, price_day_last)
        VALUES %s
        ON CONFLICT (symbol, day) DO UPDATE
        SET price_day_last = EXCLUDED.price_day_last
    """, rows, page_size=5000)
    print(f"loaded prices_daily: {len(rows)}")

def load_social_daily(cur):
    files = sorted(glob.glob(SOCIAL_DAILY_PATH))
    if not files:
        print("No social daily parquet.")
        return
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["day"] = pd.to_datetime(df["day"]).dt.date
    rows = list(df[["ticker","day","mentions"]].itertuples(index=False, name=None))
    execute_values(cur, """
        INSERT INTO social_daily(ticker, day, mentions)
        VALUES %s
        ON CONFLICT (ticker, day) DO UPDATE
        SET mentions = EXCLUDED.mentions
    """, rows, page_size=5000)
    print(f"loaded social_daily: {len(rows)}")

def main():
    c = conn()
    try:
        with c.cursor() as cur:
            load_prices_daily(cur)
            load_social_daily(cur)
        c.commit()
        print("Done.")
    finally:
        c.close()

if __name__ == "__main__":
    main()