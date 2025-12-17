import os
import time
from datetime import datetime, timezone
import glob

from prometheus_client import start_http_server, Gauge
from confluent_kafka import Consumer, TopicPartition, KafkaException
import psycopg2
import pandas as pd

# In Docker: kafka:9092 (nicht localhost)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Empfehlung: DSN als URL, z.B.
# POSTGRES_DSN="postgresql://gabi:gabi_pw@postgres:5432/gabi"
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")

# In Docker könnt ihr z.B. ein Volume nach /data mounten
LAKE_ROOT = os.getenv("LAKE_ROOT", "/data/lake")

TOPICS = {
    "prices_raw": "prices_raw",
    "social_raw": "social_raw",
}

# Prometheus metrics
kafka_topic_end_offset = Gauge(
    "dad_kafka_topic_end_offset",
    "End offset per topic partition",
    ["topic", "partition"],
)
kafka_up = Gauge("dad_kafka_up", "Kafka reachable (1/0)")
postgres_up = Gauge("dad_postgres_up", "Postgres reachable (1/0)")
lake_rows = Gauge("dad_lake_rows", "Row count for parquet dataset", ["dataset"])
lake_last_modified_seconds = Gauge(
    "dad_lake_last_modified_seconds",
    "Seconds since last file modification",
    ["dataset"],
)


def get_end_offsets(consumer: Consumer, topic: str) -> dict[int, int]:
    """
    Liefert End-Offsets pro Partition (partition -> offset).
    End-Offset = "nächster" Offset (also #messages, wenn bei 0 startend).
    """
    md = consumer.list_topics(topic=topic, timeout=5)
    if md is None or topic not in md.topics:
        return {}

    tmeta = md.topics[topic]
    if tmeta is None or tmeta.partitions is None:
        return {}

    partitions = sorted(list(tmeta.partitions.keys()))
    if not partitions:
        return {}

    tps = [TopicPartition(topic, p) for p in partitions]

    # get_watermark_offsets liefert (low, high)
    # high ist der "end offset"
    out = {}
    for tp in tps:
        low, high = consumer.get_watermark_offsets(tp, timeout=5)
        out[tp.partition] = int(high)
    return out


def parquet_row_count(path: str) -> int:
    files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)
    if not files:
        return 0

    total = 0
    for f in files[-50:]:  # cap, damit es in der Demo nicht eskaliert
        try:
            df = pd.read_parquet(f)
            total += len(df)
        except Exception:
            pass
    return total


def last_modified_seconds(path: str) -> float:
    files = glob.glob(os.path.join(path, "**/*"), recursive=True)
    files = [f for f in files if os.path.isfile(f)]
    if not files:
        return float("inf")

    latest_mtime = max(os.path.getmtime(f) for f in files)
    now = time.time()
    return max(0.0, now - latest_mtime)


def check_postgres():
    if not POSTGRES_DSN:
        postgres_up.set(0)
        return

    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        postgres_up.set(1)
    except Exception:
        postgres_up.set(0)


def build_kafka_consumer() -> Consumer | None:
    """
    Wir nutzen den Consumer nur für Metadaten + Watermarks.
    Dafür müssen wir NICHT subscriben.
    """
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "dad-monitor",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        # Wichtig in Labs/Praktikum: schnelle Fehler statt hängen
        "socket.timeout.ms": 5000,
        "session.timeout.ms": 10000,
    }
    try:
        c = Consumer(conf)
        # einfacher Reachability-Test
        c.list_topics(timeout=5)
        kafka_up.set(1)
        return c
    except Exception:
        kafka_up.set(0)
        return None


def main():
    # Prometheus Exporter
    start_http_server(8000)
    print("[metrics] exporter started on :8000/metrics")

    consumer = build_kafka_consumer()

    datasets = {
        "social_speed": os.path.join(LAKE_ROOT, "social", "reddit", "speed"),
        "social_batch_daily": os.path.join(LAKE_ROOT, "social", "reddit", "batch_daily"),
        "prices_speed": os.path.join(LAKE_ROOT, "prices", "speed"),
        "prices_batch_daily": os.path.join(LAKE_ROOT, "prices", "batch_daily"),
    }

    while True:
        # Kafka end offsets
        if consumer is None:
            consumer = build_kafka_consumer()

        if consumer:
            try:
                kafka_up.set(1)
                for _, topic in TOPICS.items():
                    ends = get_end_offsets(consumer, topic)
                    for p, off in ends.items():
                        kafka_topic_end_offset.labels(topic=topic, partition=str(p)).set(off)
            except KafkaException:
                kafka_up.set(0)
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None
            except Exception:
                kafka_up.set(0)
                # keep running, try again next loop

        else:
            kafka_up.set(0)

        # Data lake metrics
        for ds, path in datasets.items():
            lake_rows.labels(dataset=ds).set(parquet_row_count(path))
            lake_last_modified_seconds.labels(dataset=ds).set(last_modified_seconds(path))

        # Postgres ping
        check_postgres()

        time.sleep(10)


if __name__ == "__main__":
    main()