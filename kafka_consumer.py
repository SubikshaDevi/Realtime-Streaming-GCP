from confluent_kafka import Consumer
import json

# Kafka consumer config
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'clickhouse_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to Debezium topic for your table
# consumer.subscribe(['dbserver1.public.driver_availablity'])  # change topic name accordingly
consumer.subscribe(['gcppg.public.driver_availablity'])

#data-engg-kafka-1
#sudo docker exec -it data-engg-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        value = json.loads(msg.value())
        print(json.dumps(value, indent=2))  # Just to understand structure
        # You’ll extract 'payload' from this and insert into ClickHouse
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
