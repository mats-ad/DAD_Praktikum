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

def fetch_global_quote(symbol: str):
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    resp = requests.get(BASE_URL, params=params)
    return resp.json()

def fetch_daily(symbol: str):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }
    resp = requests.get(BASE_URL, params=params)
    return resp.json()

def main():
    if not API_KEY:
        raise RuntimeError("ALPHA_VANTAGE_KEY fehlt in .env")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Alpha Vantage Ingest gestartet...")

    symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]

    while True:
        for symbol in symbols:
            try:
                data = {
                    "symbol": symbol,
                    "global_quote": fetch_global_quote(symbol),
                    "daily": fetch_daily(symbol)
                }
                producer.send(TOPIC, data)
                print(f"[SEND] {symbol} -> prices_raw")
            except Exception as e:
                print(f"[ERROR] {symbol}: {e}")

        print("Sleep 60s (free API, limit ~25 requests/min)...")
        time.sleep(60)

if __name__ == "__main__":
    main()