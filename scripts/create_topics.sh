#!/usr/bin/env bash
set -e

docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic prices_raw --partitions 1 --replication-factor 1

docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic social_raw --partitions 1 --replication-factor 1

echo "[OK] topics created"