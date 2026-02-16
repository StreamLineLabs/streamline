"""Consume messages from Streamline using kafka-python (standard Kafka client)."""

import json
import os

from kafka import KafkaConsumer

bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    "demo",
    bootstrap_servers=bootstrap,
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Consuming from 'demo' topic (5s timeout)...")
count = 0
for message in consumer:
    print(f"  offset={message.offset} partition={message.partition} value={message.value}")
    count += 1

consumer.close()
print(f"Done! Consumed {count} messages.")
