import os
import json
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "prices_raw"

SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "V", "MA", "BAC",
    "WMT", "COST", "NKE",
    "JNJ", "PFE",
    "XOM", "CVX", "BA",
    "GME"
]

POLL_SECONDS = 30  # alle 30s reicht locker


def fetch_price(symbol: str) -> float | None:
    """
    Robust: versucht mehrere Felder, weil Yahoo je nach Symbol/Markt variiert.
    """
    try:
        t = yf.Ticker(symbol)

        # 1) "fast_info" ist häufig am zuverlässigsten
        fi = getattr(t, "fast_info", None)
        if fi:
            p = fi.get("last_price") or fi.get("lastPrice")
            if p is not None:
                return float(p)

        # 2) Fallback über info
        info = t.info or {}
        p = info.get("regularMarketPrice") or info.get("currentPrice")
        if p is not None:
            return float(p)

        # 3) Letzter Fallback: 1m-history
        hist = t.history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])

        return None
    except Exception:
        return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(3, 5, 0),
    )

    print("Yahoo Finance ingest gestartet…")

    while True:
        for symbol in SYMBOLS:
            price = fetch_price(symbol)

            event = {
                "symbol": symbol,
                "price": price,                 # kann None sein
                "source": "yahoo",
                "ingest_ts": datetime.now(timezone.utc).isoformat()
            }

            producer.send(TOPIC, event)
            print(f"[SEND] {symbol} price={price}")

            time.sleep(1)  # kleine Pause

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()