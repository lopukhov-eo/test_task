import configparser
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

config_path = Path(__file__).resolve().parent.parent / "config.ini"

config = configparser.ConfigParser()
config.read(config_path)

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD")

CLICKHOUSE_HOST = config["ClickHouse"]["host"]
CLICKHOUSE_PORT = config["ClickHouse"].getint("port") or "9000"

CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
CLICKHOUSE_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent / config["ClickHouse"]["schema_path"]
)
CLICKHOUSE_SCHEMA_NAME = config["ClickHouse"]["db_schema_name"]

BATCH_SIZE = config["ClickHouse"].getint("batch_size")
