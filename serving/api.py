import os
import psycopg2
from fastapi import FastAPI, HTTPException

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "gabi")
PG_USER = os.getenv("PG_USER", "gabi")
PG_PASSWORD = os.getenv("PG_PASSWORD", "gabi_pw")

app = FastAPI(title="GABI Serving API (Postgres)")

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )

@app.get("/health")
def health():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            ok = cur.fetchone()[0] == 1
        conn.close()
        return {"ok": True, "postgres": ok, "db": PG_DB}
    except Exception as e:
        return {"ok": False, "postgres": False, "error": str(e), "db": PG_DB}

@app.get("/prices/daily/{symbol}")
def daily_prices(symbol: str, limit: int = 30):
    limit = max(1, min(int(limit), 365))
    q = """
    SELECT day, price_day_last
    FROM prices_daily
    WHERE symbol = %s
    ORDER BY day DESC
    LIMIT %s
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(q, (symbol, limit))
            rows = cur.fetchall()
        conn.close()
        return [{"day": str(d), "price_day_last": p} for d, p in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/social/daily/{ticker}")
def daily_mentions(ticker: str, limit: int = 30):
    limit = max(1, min(int(limit), 365))
    q = """
    SELECT day, mentions
    FROM social_daily
    WHERE ticker = %s
    ORDER BY day DESC
    LIMIT %s
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(q, (ticker, limit))
            rows = cur.fetchall()
        conn.close()
        return [{"day": str(d), "mentions": int(m)} for d, m in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))