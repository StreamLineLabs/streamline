# External Kafka Client Examples

These examples prove that standard Kafka client libraries work with Streamline unchanged.
Start Streamline first, then run any example below.

## Prerequisites

```bash
# Start Streamline with demo topics
streamline --playground
```

## Python (kafka-python)

```bash
cd python
pip install -r requirements.txt
python producer.py
python consumer.py
```

## Node.js (KafkaJS)

```bash
cd nodejs
npm install
node producer.js
node consumer.js
```

## Go (franz-go)

```bash
cd go
go run producer.go
go run consumer.go
```

## What This Proves

Every example connects to `localhost:9092` using a standard Kafka client library with
zero Streamline-specific configuration. If your application already uses Kafka, it works
with Streamline as a drop-in replacement.
