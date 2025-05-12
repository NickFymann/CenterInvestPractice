import requests
from clickhouse_connect import get_client
from types import SimpleNamespace

client = get_client(host='clickhouse', port=8123, username='', password='', database='')

client.command("CREATE DATABASE IF NOT EXISTS PROJECT;")
client.command("USE PROJECT;")
client.command("""
CREATE TABLE IF NOT EXISTS publications (
    id Int32,
    parent_id Int32,
    category_name String,
    no_active Int8
) ENGINE = MergeTree()
ORDER BY id;
""")

client.command("""
CREATE TABLE IF NOT EXISTS datasets (
    id Int32,
    publication_id Int32,
    name String,
    type Int8,
    reporting String,
    full_name String,
    link String,
    updated_time DateTime
) ENGINE = MergeTree()
ORDER BY id;
""")

client.command("""
CREATE TABLE IF NOT EXISTS measures (
    id Int32,
    dataset_id Int32,
    name String,
    parent_id Int32,
    sort Int32
) ENGINE = MergeTree()
ORDER BY id;
""")

client.command("""
CREATE TABLE IF NOT EXISTS units (
    id Int32,
    val String
) ENGINE = MergeTree()
ORDER BY id;
""")

client.command("""
CREATE TABLE IF NOT EXISTS year_ranges (
    dataset_id Int32,
    measure_id Int32,
    from_year Int16,
    to_year Int16
) ENGINE = MergeTree()
ORDER BY (dataset_id, measure_id);
""")

client.command("""
CREATE TABLE IF NOT EXISTS data (
    dataset_id Int32,
    measure_id Int32,
    unit_id Nullable(Int32),
    obs_val Nullable(Decimal(18, 6)),
    row_id Nullable(Int32),
    dt Nullable(String),
    periodicity Nullable(String),
    col_id Nullable(Int32),
    date Date,
    digits Nullable(Int8)
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (dataset_id, measure_id, date);
""")

client.command("""
CREATE TABLE IF NOT EXISTS load_progress (
    publication_id Int32,
    dataset_id Int32,
    measure_id Int32
) ENGINE = MergeTree()
ORDER BY (publication_id, dataset_id, measure_id);
""")