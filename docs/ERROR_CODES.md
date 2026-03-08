# Error Codes

Streamline uses standard Kafka error codes plus additional codes for
Streamline-specific features.

## Standard Kafka Error Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | NONE | Success |
| 1 | OFFSET_OUT_OF_RANGE | Requested offset is out of range |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Topic or partition does not exist |
| 6 | NOT_LEADER_OR_FOLLOWER | Broker is not the leader for this partition |
| 7 | REQUEST_TIMED_OUT | Request timed out |

## Streamline-Specific Error Codes

| Code | Name | Description |
|------|------|-------------|
| 1001 | QUERY_SYNTAX_ERROR | Invalid StreamQL query syntax |
| 1002 | SCHEMA_VALIDATION_FAILED | Message does not match registered schema |
| 1003 | TRANSFORM_ERROR | WASM transform execution failed |
| 1004 | RATE_LIMITED | Client exceeded configured rate limit |
