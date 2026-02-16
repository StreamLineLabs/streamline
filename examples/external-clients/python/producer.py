"""Produce messages to Streamline using kafka-python (standard Kafka client)."""

import json
import os

from kafka import KafkaProducer

bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(5):
    msg = {"event": "test", "index": i}
    future = producer.send("demo", value=msg)
    metadata = future.get(timeout=10)
    print(f"Sent to partition={metadata.partition} offset={metadata.offset}: {msg}")

producer.flush()
producer.close()
print("Done! 5 messages produced to 'demo' topic.")
