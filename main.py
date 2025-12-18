import subprocess
import sys
import time
import signal
from pathlib import Path

ROOT = Path(__file__).parent

SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def run(cmd, name, wait=False):
    print(f"[START] {name}: {' '.join(map(str, cmd))}")
    p = subprocess.Popen(cmd)
    if wait:
        rc = p.wait()
        if rc != 0:
            raise RuntimeError(f"{name} exited with code {rc}")
    return p


def run_spark(script: Path, name: str):
    return run(
        ["spark-submit", "--packages", SPARK_KAFKA_PKG, str(script)],
        name,
    )


def run_batch(script: Path, name: str):
    print(f"[BATCH] {name}")
    subprocess.run(
        ["spark-submit", "--packages", SPARK_KAFKA_PKG, str(script)],
        check=False,
    )


def main():
    procs = []

    try:
        # 1) Infra
        print("[BOOT] starting docker infrastructure...")
        #subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
        # subprocess.run(["docker", "compose", "start"], check=True)

        # 2) Topics
        print("[BOOT] creating kafka topics...")
        for topic in ["prices_raw", "social_raw"]:
            subprocess.run(
                [
                    "docker", "compose", "exec", "-T", "kafka",
                    "kafka-topics",
                    "--bootstrap-server", "localhost:9092",
                    "--create", "--if-not-exists",
                    "--topic", topic,
                    "--partitions", "1",
                    "--replication-factor", "1",
                ],
                check=False,
            )
        print("[OK] kafka topics ready")

        # 3) Ingest
        print("[BOOT] starting ingest layer...")
        procs.append(run([sys.executable, "ingest/yahoo_prices_ingest.py"], "yahoo_prices_ingest"))
        procs.append(run([sys.executable, "ingest/reddit_ingest.py"], "reddit_ingest"))

        # 4) Speed Layer
        print("[BOOT] starting speed layer...")
        procs.append(run_spark(ROOT / "speed/prices_speed.py", "prices_speed"))
        procs.append(run_spark(ROOT / "speed/social_speed.py", "social_speed"))

        print("[OK] Lambda pipeline running (ingest + speed)")
        print("[INFO] Batch jobs will run every 5 minutes and load into Postgres.")
        print("[INFO] Press Ctrl+C to stop")

        # 5) Periodic Batch + Load to Postgres
        while True:
            time.sleep(300)  # alle 5 Minuten (Demo)

            print("\n[BATCH] running batch jobs...")
            run_batch(ROOT / "batch/prices_batch_daily.py", "prices_batch_daily")
            run_batch(ROOT / "batch/social_batch_daily.py", "social_batch_daily")

            print("[LOAD] loading batch results into Postgres...")
            subprocess.run([sys.executable, "serving/load_to_postgres.py"], check=False)

            print("[BATCH+LOAD] finished\n")

    except KeyboardInterrupt:
        print("\n[STOP] shutting down...")

    finally:
        for p in procs:
            try:
                p.send_signal(signal.SIGTERM)
            except Exception:
                pass


if __name__ == "__main__":
    main()