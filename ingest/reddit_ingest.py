import os, json, time
import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "social_raw"

USER_AGENT = "gabi-dad-project/0.1 (contact: mats.adel@haw-hamburg.de)"  
SUBREDDITS = ["stocks", "investing", "wallstreetbets"]

def fetch_listing(subreddit: str, listing: str = "new", limit: int = 50):
    url = f"https://www.reddit.com/r/{subreddit}/{listing}.json"
    headers = {"User-Agent": USER_AGENT}
    params = {"limit": limit}
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(3, 5, 0),
    )   

    seen = set()
    print("Reddit public scraper gestartet…")

    while True:
        for sub in SUBREDDITS:
            try:
                data = fetch_listing(sub, "new", 50)
                children = data.get("data", {}).get("children", [])
            except Exception as e:
                print(f"[ERROR] {sub}: {e}")
                continue

            for c in children:
                d = c.get("data", {})
                post_id = d.get("id")
                if not post_id:
                    continue
                key = f"{sub}_{post_id}"
                if key in seen:
                    continue
                seen.add(key)

                event = {
                    "source": "reddit",
                    "subreddit": sub,
                    "id": post_id,
                    "created_utc": d.get("created_utc"),
                    "title": d.get("title", ""),
                    "text": d.get("selftext", ""),
                    "score": d.get("score"),
                    "num_comments": d.get("num_comments"),
                    "permalink": d.get("permalink"),
                    # optional: URL, author, etc.
                }

                producer.send(TOPIC, event)
                print(f"[SEND] {key} -> {TOPIC}")

            # höflich: nicht dauerfeuern
            time.sleep(10)

        # einmal pro Runde etwas länger pausieren
        time.sleep(20)

if __name__ == "__main__":
    main()