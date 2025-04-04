# Real-Time Data Pipeline: GCP Postgres ➔ Kafka ➔ Debezium ➔ Python Consumer ➔ ClickHouse

This document outlines the full setup of a real-time streaming pipeline:

- PostgreSQL on GCP Cloud SQL
- Kafka with Debezium connector
- Python Kafka consumer
- Final storage in ClickHouse

It also details all errors encountered and how they were solved.

---

## 🚀 Goal
Build a real-time data pipeline to stream data from GCP PostgreSQL into ClickHouse via Kafka and Python.

---

## 📂 Tools & Services
- GCP Cloud SQL (PostgreSQL)
- Docker Compose
- Apache Kafka (Confluent)
- Debezium PostgreSQL Source Connector
- Python (confluent_kafka)
- ClickHouse

---

## 📁 Architecture
```
GCP PostgreSQL  
      ⬇️  (CDC via Debezium)  
Kafka Broker (Topic: gcppg.public.driver_availablity)
      ⬇️
Python Kafka Consumer
      ⬇️
ClickHouse Table
```

---

## 🤖 Step-by-Step Process

### 1. 🌐 Enable Logical Replication in GCP PostgreSQL
```sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
SELECT pg_reload_conf();
```

Create a user with replication privilege:
```sql
CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'yourpassword';
```

### 2. 🚚 Docker Compose Setup for Kafka, Zookeeper, Debezium
Key points in `docker-compose.yml`:
```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: data-engg-kafka-1
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092

  connect:
    image: debezium/connect:2.5
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      CONFIG_STORAGE_TOPIC: debezium_connect_config
```

### 3. 🎓 Debugging Docker + Debezium Issues
- **Debezium exited with `TimeoutException: listNodes`**
  ➡️ Ensure `BOOTSTRAP_SERVERS` is `kafka:9092` not IP.

- **`psql` not found inside Debezium**
  ➡️ Use a local `psql` CLI or run from another Postgres container.

- **Connector returns error: `topic.prefix is required`**
  ➡️ Ensure `"topic.prefix": "gcppg"` is set in `postgres-source.json`.

- **Error: `Subscribed topic not available`**
  ➡️ Confirm topic name via:
```bash
sudo docker exec -it data-engg-kafka-1 /usr/bin/kafka-topics --bootstrap-server kafka:29092 --list
```


---

## 🚧 Debezium Connector Config (`postgres-source.json`)
```json
{
  "name": "source-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "<GCP_POSTGRES_IP>",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "yourpassword",
    "database.dbname": "your_db_name",
    "database.server.name": "gcppg",
    "plugin.name": "pgoutput",
    "topic.prefix": "gcppg",
    "table.include.list": "public.driver_availablity",
    "slot.name": "driver_availablity_slot"
  }
}
```
Apply with:
```bash
curl -X POST -H "Content-Type: application/json" \
     --data @postgres-source.json \
     http://localhost:8083/connectors
```

---

## 🧄 Python Kafka Consumer
```python
from confluent_kafka import Consumer
import json
import logging

logging.basicConfig(level=logging.INFO)

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'clickhouse_consumer_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['gcppg.public.driver_availablity'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        value = json.loads(msg.value())
        logging.info(json.dumps(value, indent=2))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

---

## 🚪 ClickHouse Setup
Expose ports `8123` and `9000`:
```yaml
  clickhouse:
    image: clickhouse/clickhouse-server
    container_name: my-clickhouse-server
    ports:
      - "8123:8123"
      - "9000:9000"
```

Use SQLAlchemy to write to ClickHouse.

### Insert Example:
```python
from sqlalchemy import create_engine
from clickhouse_driver import Client
import pandas as pd

engine = create_engine('clickhouse+native://default:@localhost/default')

def execute_query(query):
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        print(df)
    return df

def insert_clickhouse():
    insert_query = """
        INSERT INTO driver_availablity SELECT * FROM driver_availablity_pg;
    """
    execute_query(insert_query)
```

---

## 🚫 Common Errors & Fixes

| Error | Cause | Fix |
|------|-------|-----|
| `No replication slots` | Replication not configured | Run `ALTER SYSTEM SET wal_level = logical` |
| `Connection refused` | Wrong Kafka/ClickHouse host in Docker | Use correct container name or bridge network |
| `UNKNOWN_TOPIC_OR_PART` | Topic not created yet | Check topic via `kafka-topics.sh --list` |
| `psql: not found` | Debezium image is minimal | Use external psql CLI |
| `kafka-console-consumer.sh not found` | Kafka binary path missing | Use full path `/usr/bin/kafka-console-consumer` |
| `debezium exited` | Wrong BOOTSTRAP_SERVERS or Kafka isn't ready | Ensure service name is used |

---

## 🌟 Final Checklist

- [x] Postgres logical replication setup
- [x] Debezium running and connected
- [x] Kafka receiving messages
- [x] Python consumer processing topic data
- [x] ClickHouse accepting writes

---

## 🎉 Congratulations!
You’ve built a production-ready **real-time CDC pipeline**.



