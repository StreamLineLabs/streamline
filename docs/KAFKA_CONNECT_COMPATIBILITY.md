# Kafka Connect Compatibility Matrix

## Tested Connectors

| Connector | Type | Status | Notes |
|-----------|------|--------|-------|
| FileStream Source | Source | 🟡 Planned | Built-in test connector |
| FileStream Sink | Sink | 🟡 Planned | Built-in test connector |
| JDBC Source | Source | 🟡 Planned | Requires JDBC driver |
| JDBC Sink | Sink | 🟡 Planned | Requires JDBC driver |
| S3 Sink | Sink | 🟡 Planned | Confluent S3 connector |
| Elasticsearch Sink | Sink | 🟡 Planned | Confluent ES connector |
| Debezium PostgreSQL | Source | 🟡 Planned | CDC connector |
| Debezium MySQL | Source | 🟡 Planned | CDC connector |
| HTTP Sink | Sink | 🟡 Planned | Community connector |
| BigQuery Sink | Sink | 🟡 Planned | WePay connector |

## REST API Compatibility

| Endpoint | Status | Notes |
|----------|--------|-------|
| `GET /connectors` | ✅ Implemented | Lists all connectors |
| `POST /connectors` | ✅ Implemented | Creates a connector |
| `GET /connectors/{name}` | ✅ Implemented | Gets connector info |
| `PUT /connectors/{name}/config` | ✅ Implemented | Updates config |
| `DELETE /connectors/{name}` | ✅ Implemented | Deletes connector |
| `GET /connectors/{name}/status` | ✅ Implemented | Gets status |
| `POST /connectors/{name}/restart` | ✅ Implemented | Restarts connector |
| `PUT /connectors/{name}/pause` | ✅ Implemented | Pauses connector |
| `PUT /connectors/{name}/resume` | ✅ Implemented | Resumes connector |
| `GET /connector-plugins` | ✅ Implemented | Lists plugins |

## Running Compatibility Tests

```bash
# Start test environment
docker compose -f docker/docker-compose.benchmark.yml up -d streamline

# Test with a specific connector
curl -X POST http://localhost:9094/connectors \
  -H "Content-Type: application/json" \
  -d '{"name": "test-file-source", "config": {"connector.class": "FileStreamSource", "file": "/tmp/test.txt", "topic": "file-topic"}}'

# Check status
curl http://localhost:9094/connectors/test-file-source/status
```
