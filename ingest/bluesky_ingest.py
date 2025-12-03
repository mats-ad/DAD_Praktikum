import os
import json
import time
from typing import List, Dict
import re

from atproto import Client
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "social_raw"

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

TICKERS = {"AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"}

DOLLAR_PATTERN = re.compile(r"\$([A-Z]{1,5})")
WORD_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")


def get_client() -> Client:
    if not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD:
        raise RuntimeError("BLUESKY_HANDLE oder BLUESKY_APP_PASSWORD fehlt in .env")

    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
    return client


def extract_tickers(text: str) -> List[str]:
    if not text:
        return []

    candidates = set(m.group(1) for m in DOLLAR_PATTERN.finditer(text))
    candidates |= set(WORD_PATTERN.findall(text))
    return [t for t in candidates if t in TICKERS]


def fetch_timeline_posts(client: Client, limit: int = 20) -> List[Dict]:
    timeline = client.app.bsky.feed.get_timeline({"limit": limit})
    posts = []

    for item in timeline.feed:
        post = item.post
        record = post.record

        text = getattr(record, "text", "")
        created_at = getattr(record, "createdAt", None)

        tickers = extract_tickers(text)
        if not tickers:
            continue

        posts.append(
            {
                "source": "bluesky",
                "uri": post.uri,
                "cid": post.cid,
                "author_did": post.author.did,
                "author_handle": post.author.handle,
                "text": text,
                "created_at": created_at,
                "tickers": tickers,
                "like_count": getattr(post, "likeCount", None),
                "repost_count": getattr(post, "repostCount", None),
                "reply_count": getattr(post, "replyCount", None),
            }
        )

    return posts


def main():
    client = get_client()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Bluesky Ingest gestartet…")

    seen = set()

    while True:
        try:
            posts = fetch_timeline_posts(client, limit=50)
        except Exception as e:
            print(f"[ERROR] fetch_timeline: {e}")
            time.sleep(30)
            continue

        for p in posts:
            key = p["uri"]
            if key in seen:
                continue
            seen.add(key)

            producer.send(TOPIC, p)
            print(f"[SEND] {p['author_handle']} {p['tickers']} -> {TOPIC}")

        print("Sleep 30s…")
        time.sleep(30)


if __name__ == "__main__":
    main()
