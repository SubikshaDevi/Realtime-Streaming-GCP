import json
import logging
from datetime import datetime
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

import os

# Load environment variables from .env file
load_dotenv()

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
# --------- SETUP ----------

engine = create_engine(
    f"clickhouse+http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

def execute_query(query):
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        print(df)
    return df


KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'gcppg.public.driver_availablity'

# --------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --------- UTILS ----------
def execute_query(query, params=None):
    try:
        with engine.connect() as connection:
            if params:
                connection.execute(text(query), params)
            else:
                connection.execute(text(query))
            logging.info("✅ Query executed")
    except Exception as e:
        logging.error(f"❌ Error executing query: {e}")

def insert_into_clickhouse(row):
    insert_query = """
        INSERT INTO driver_availablity (id, driver_id, location_id, status, created_at)
        VALUES (:id, :driver_id, :location_id, :status, :created_at)
    """
    params = {
        'id': row[0],
        'driver_id': row[1],
        'location_id': row[2],
        'status': row[3],
        'created_at': row[4]
    }
    execute_query(insert_query, params)

from datetime import datetime, timezone, timedelta
import pytz

def convert_debezium_timestamp(microseconds):
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.fromtimestamp(microseconds / 1_000_000, tz=ist)

# --------- KAFKA CONSUMER ----------
def start_consumer():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'clickhouse_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info("📡 Listening for events...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value())
                payload = value.get("payload", {})
                after = payload.get("after")
                if not after:
                    continue

                created_at = after['created_at']
                created_at = convert_debezium_timestamp(created_at)
                
                row = (
                    int(after['id']),
                    int(after['driver_id']),
                    int(after['location_id']),
                    after['status'],
                    created_at
                )
                print(row)
                insert_into_clickhouse(row)

            except Exception as e:
                logging.warning(f"⚠️ Skipped malformed message: {e}")

    except KeyboardInterrupt:
        logging.info("🛑 Gracefully stopping...")
    finally:
        consumer.close()
        logging.info("✅ Consumer closed.")

# --------- ENTRY ----------
if __name__ == "__main__":
    start_consumer()
