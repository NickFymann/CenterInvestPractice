#!/bin/bash
echo "Waiting for ClickHouse to be ready..."

until curl -s http://clickhouse:8123/ >/dev/null; do
  echo "ClickHouse is not ready yet..."
  sleep 2
done

echo "ClickHouse is ready. Creating schema..."
python create_schema.py

echo "Schema created. Loading data..."
python load_data.py
