import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "prices_raw"
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

BASE_URL = "https://www.alphavantage.co/query"


def fetch_global_quote(symbol: str) -> float | None:
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    r = requests.get(BASE_URL, params=params, timeout=15)
    data = r.json()

    try:
        return float(data["Global Quote"]["05. price"])
    except Exception:
        return None


def main():
    if not API_KEY:
        raise RuntimeError("ALPHA_VANTAGE_KEY fehlt in .env")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]

    print("Alpha Vantage Ingest gestartet…")

    while True:
        for symbol in symbols:
            price = fetch_global_quote(symbol)

            event = {
                "symbol": symbol,
                "price": price,          # None, wenn Free Tier blockiert
                "source": "alpha_vantage"
            }

            producer.send(TOPIC, event)
            print(f"[SEND] {symbol} price={price}")

            time.sleep(3)  # Rate Limit schonen

        print("Sleep 60s…")
        time.sleep(60)


if __name__ == "__main__":
    main()