# Implement Proper Kafka Error Codes in Protocol Responses

## Priority: High

## Summary

Kafka clients expect specific numeric error codes in protocol responses. Currently, some error paths return generic errors or error strings instead of proper Kafka error codes, which can confuse clients and break retry logic.

## Current State

- `src/protocol/kafka.rs` handles most error codes correctly
- Some error paths may not map to correct Kafka error codes
- Error messages are strings rather than structured codes
- Client retry logic may not work correctly for some errors

## Requirements

### Kafka Error Codes to Support

| Code | Name | When to Use |
|------|------|-------------|
| 0 | NONE | Success |
| 1 | OFFSET_OUT_OF_RANGE | Requested offset doesn't exist |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Topic/partition doesn't exist |
| 6 | NOT_LEADER_FOR_PARTITION | Request sent to non-leader |
| 7 | REQUEST_TIMED_OUT | Operation timed out |
| 10 | MESSAGE_TOO_LARGE | Message exceeds max size |
| 17 | INVALID_REQUIRED_ACKS | Invalid acks parameter |
| 19 | REBALANCE_IN_PROGRESS | Consumer group rebalancing |
| 25 | UNKNOWN_MEMBER_ID | Consumer not in group |
| 27 | ILLEGAL_GENERATION | Wrong generation ID |
| 36 | TOPIC_ALREADY_EXISTS | Topic creation conflict |
| 37 | INVALID_PARTITIONS | Invalid partition count |
| 41 | NOT_ENOUGH_REPLICAS | ISR below min.insync.replicas |
| 58 | THROTTLING_QUOTA_EXCEEDED | Rate limit exceeded |
| 74 | SASL_AUTHENTICATION_FAILED | Auth failure |

### Implementation Tasks

1. **Audit current error handling**
   - Review all response builders in `src/protocol/kafka.rs`
   - Map `StreamlineError` variants to Kafka error codes
   - Identify any missing mappings

2. **Create error code mapping**
   ```rust
   impl From<&StreamlineError> for i16 {
       fn from(err: &StreamlineError) -> i16 {
           match err {
               StreamlineError::TopicNotFound(_) => 3,  // UNKNOWN_TOPIC
               StreamlineError::PartitionNotFound(_) => 3,
               StreamlineError::InvalidOffset(_) => 1,  // OFFSET_OUT_OF_RANGE
               StreamlineError::NotLeader(_) => 6,
               StreamlineError::AuthenticationFailed(_) => 74,
               // ... etc
           }
       }
   }
   ```

3. **Update response builders**
   - Ensure all error responses include correct error code
   - Include error message in response where protocol allows
   - Test with real Kafka clients

4. **Add retriable error indication**
   ```rust
   impl StreamlineError {
       pub fn is_retriable(&self) -> bool {
           matches!(self,
               StreamlineError::NotLeader(_) |
               StreamlineError::Replication(_) |
               // ... other retriable errors
           )
       }
   }
   ```

### Testing

```rust
#[tokio::test]
async fn test_unknown_topic_returns_correct_error_code() {
    let server = TestServer::start().await;
    let client = KafkaClient::connect(&server.addr).await;

    let response = client.fetch("nonexistent-topic", 0, 0).await;

    assert_eq!(response.error_code, 3); // UNKNOWN_TOPIC_OR_PARTITION
}
```

### Acceptance Criteria

- [ ] All error paths return correct Kafka error codes
- [ ] Client retry logic works correctly (retriable vs non-retriable)
- [ ] Error messages included where protocol supports
- [ ] Tests verify error codes for common scenarios
- [ ] librdkafka, kafka-python, and franz-go handle errors correctly

## Related Files

- `src/protocol/kafka.rs` - Protocol handling
- `src/error.rs` - Error types
- `kafka-protocol` crate - Error code definitions

## Labels

`high-priority`, `protocol`, `compatibility`, `production-readiness`
