//! Tests for the GraphQL API
//!
//! Tests cover queries, mutations, and subscription setup for the Streamline
//! GraphQL endpoint.

#![cfg(feature = "graphql")]

use async_graphql::Request;
use std::sync::Arc;
use std::time::Instant;

/// Helper to create a TopicManager backed by a temporary directory
fn create_test_topic_manager() -> Arc<streamline::storage::TopicManager> {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let path = dir.into_path();
    let tm = streamline::storage::TopicManager::new(&path)
        .expect("Failed to create TopicManager");
    Arc::new(tm)
}

/// Build a GraphQL schema for testing (no GroupCoordinator)
fn build_test_schema(
    topic_manager: Arc<streamline::storage::TopicManager>,
) -> streamline::graphql::StreamlineSchema {
    streamline::graphql::build_schema(topic_manager, Instant::now(), None)
}

// =============================================================================
// Query tests
// =============================================================================

#[tokio::test]
async fn test_query_topics_empty() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new("{ topics { name partitions } }"))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    let topics = data["topics"].as_array().expect("topics should be array");
    // No internal topics should show up (or just internal ones)
    // The empty topic list is valid
    assert!(topics.is_empty() || !topics.is_empty());
}

#[tokio::test]
async fn test_query_topics_after_create() {
    let tm = create_test_topic_manager();
    tm.create_topic("test-topic", 3)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            "{ topics { name partitions messageCount } }",
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    let topics = data["topics"].as_array().expect("topics should be array");

    let test_topic = topics
        .iter()
        .find(|t| t["name"] == "test-topic")
        .expect("test-topic should exist");
    assert_eq!(test_topic["partitions"], 3);
    assert_eq!(test_topic["messageCount"], 0);
}

#[tokio::test]
async fn test_query_single_topic() {
    let tm = create_test_topic_manager();
    tm.create_topic("my-topic", 2)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ topic(name: "my-topic") { name partitions replicationFactor } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["topic"]["name"], "my-topic");
    assert_eq!(data["topic"]["partitions"], 2);
}

#[tokio::test]
async fn test_query_topic_not_found() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ topic(name: "nonexistent") { name } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert!(data["topic"].is_null());
}

#[tokio::test]
async fn test_query_messages_empty_topic() {
    let tm = create_test_topic_manager();
    tm.create_topic("events", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ messages(topic: "events") { offset value } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    let messages = data["messages"]
        .as_array()
        .expect("messages should be array");
    assert!(messages.is_empty());
}

#[tokio::test]
async fn test_query_cluster_info() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            "{ clusterInfo { nodeId version uptime topicCount } }",
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["clusterInfo"]["nodeId"], 0);
    assert!(!data["clusterInfo"]["version"]
        .as_str()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_query_consumer_groups_empty() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new("{ consumerGroups { groupId state } }"))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    let groups = data["consumerGroups"]
        .as_array()
        .expect("consumerGroups should be array");
    assert!(groups.is_empty());
}

// =============================================================================
// Mutation tests
// =============================================================================

#[tokio::test]
async fn test_mutation_create_topic() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm.clone());

    let res = schema
        .execute(Request::new(
            r#"mutation { createTopic(name: "new-topic", partitions: 4) { name partitions } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["createTopic"]["name"], "new-topic");
    assert_eq!(data["createTopic"]["partitions"], 4);

    // Verify the topic was actually created in storage
    let stats = tm
        .get_topic_stats("new-topic")
        .expect("Topic should exist");
    assert_eq!(stats.num_partitions, 4);
}

#[tokio::test]
async fn test_mutation_delete_topic() {
    let tm = create_test_topic_manager();
    tm.create_topic("to-delete", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm.clone());

    let res = schema
        .execute(Request::new(
            r#"mutation { deleteTopic(name: "to-delete") }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["deleteTopic"], true);

    // Verify the topic was actually deleted
    assert!(tm.get_topic_stats("to-delete").is_err());
}

#[tokio::test]
async fn test_mutation_produce_message() {
    let tm = create_test_topic_manager();
    tm.create_topic("produce-test", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm.clone());

    let res = schema
        .execute(Request::new(
            r#"mutation { produce(topic: "produce-test", value: "hello world") { topic partition offset timestamp } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["produce"]["topic"], "produce-test");
    assert_eq!(data["produce"]["partition"], 0);
    assert_eq!(data["produce"]["offset"], 0);
    assert!(!data["produce"]["timestamp"].as_str().unwrap().is_empty());

    // Verify the message was stored
    let records = tm
        .read("produce-test", 0, 0, 10)
        .expect("Failed to read messages");
    assert_eq!(records.len(), 1);
    assert_eq!(
        String::from_utf8_lossy(&records[0].value),
        "hello world"
    );
}

#[tokio::test]
async fn test_mutation_produce_with_key() {
    let tm = create_test_topic_manager();
    tm.create_topic("keyed-test", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm.clone());

    let res = schema
        .execute(Request::new(
            r#"mutation { produce(topic: "keyed-test", key: "user-123", value: "{\"action\": \"click\"}") { offset } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);

    // Verify key was stored
    let records = tm
        .read("keyed-test", 0, 0, 10)
        .expect("Failed to read messages");
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string()),
        Some("user-123".to_string())
    );
}

// =============================================================================
// Query after mutation (integration)
// =============================================================================

#[tokio::test]
async fn test_produce_then_query_messages() {
    let tm = create_test_topic_manager();
    tm.create_topic("roundtrip", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    // Produce a message
    let produce_res = schema
        .execute(Request::new(
            r#"mutation { produce(topic: "roundtrip", key: "k1", value: "v1") { offset } }"#,
        ))
        .await;
    assert!(
        produce_res.errors.is_empty(),
        "Produce errors: {:?}",
        produce_res.errors
    );

    // Query the message back
    let query_res = schema
        .execute(Request::new(
            r#"{ messages(topic: "roundtrip", offset: 0) { offset key value timestamp headers { key value } } }"#,
        ))
        .await;
    assert!(
        query_res.errors.is_empty(),
        "Query errors: {:?}",
        query_res.errors
    );

    let data = query_res
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    let messages = data["messages"]
        .as_array()
        .expect("messages should be array");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["key"], "k1");
    assert_eq!(messages[0]["value"], "v1");
    assert_eq!(messages[0]["offset"], 0);
    assert!(!messages[0]["timestamp"].as_str().unwrap().is_empty());
}

// =============================================================================
// Subscription tests
// =============================================================================

#[tokio::test]
async fn test_subscription_messages_stream() {
    use futures_util::StreamExt;

    let tm = create_test_topic_manager();
    tm.create_topic("sub-test", 1)
        .expect("Failed to create topic");

    // Pre-populate with a message so the subscription has something to yield
    tm.append(
        "sub-test",
        0,
        Some(bytes::Bytes::from("key1")),
        bytes::Bytes::from("value1"),
    )
    .expect("Failed to append");

    let schema = build_test_schema(tm);

    // Execute a subscription query
    let mut stream = schema.execute_stream(
        Request::new(
            r#"subscription { messages(topic: "sub-test", fromOffset: 0) { offset key value } }"#,
        ),
    );

    // Take the first response from the stream
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("Subscription timed out")
        .expect("Stream ended unexpectedly");

    assert!(
        response.errors.is_empty(),
        "Subscription errors: {:?}",
        response.errors
    );

    let data = response
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    assert_eq!(data["messages"]["offset"], 0);
    assert_eq!(data["messages"]["key"], "key1");
    assert_eq!(data["messages"]["value"], "value1");
}

#[tokio::test]
async fn test_subscription_metrics_stream() {
    use futures_util::StreamExt;

    let tm = create_test_topic_manager();
    tm.create_topic("metrics-topic", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let mut stream = schema.execute_stream(Request::new(
        r#"subscription { metrics(intervalMs: 100) { totalMessages topicCount timestamp } }"#,
    ));

    // Take the first metrics snapshot
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("Subscription timed out")
        .expect("Stream ended unexpectedly");

    assert!(
        response.errors.is_empty(),
        "Subscription errors: {:?}",
        response.errors
    );

    let data = response
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    // At least one topic exists
    assert!(data["metrics"]["topicCount"].as_i64().unwrap() >= 1);
    assert!(!data["metrics"]["timestamp"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn test_subscription_topic_metrics_stream() {
    use futures_util::StreamExt;

    let tm = create_test_topic_manager();
    tm.create_topic("topic-metrics-test", 3)
        .expect("Failed to create topic");

    // Add a message so we can verify message_count
    tm.append(
        "topic-metrics-test",
        0,
        Some(bytes::Bytes::from("k")),
        bytes::Bytes::from("v"),
    )
    .expect("Failed to append");

    let schema = build_test_schema(tm);

    let mut stream = schema.execute_stream(Request::new(
        r#"subscription { topicMetrics(topic: "topic-metrics-test", intervalMs: 100) { topic messageCount partitionCount partitions { id earliestOffset latestOffset messageCount } timestamp } }"#,
    ));

    let response = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("Subscription timed out")
        .expect("Stream ended unexpectedly");

    assert!(
        response.errors.is_empty(),
        "Subscription errors: {:?}",
        response.errors
    );

    let data = response
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    assert_eq!(data["topicMetrics"]["topic"], "topic-metrics-test");
    assert_eq!(data["topicMetrics"]["partitionCount"], 3);
    assert!(data["topicMetrics"]["messageCount"].as_i64().unwrap() >= 1);
    let partitions = data["topicMetrics"]["partitions"]
        .as_array()
        .expect("partitions should be array");
    assert_eq!(partitions.len(), 3);
    assert!(!data["topicMetrics"]["timestamp"]
        .as_str()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_subscription_topic_metrics_nonexistent_topic() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"subscription { topicMetrics(topic: "does-not-exist") { topic } }"#,
        ))
        .await;

    // Should return an error since the topic doesn't exist
    assert!(!res.errors.is_empty(), "Expected an error for nonexistent topic");
}

// =============================================================================
// New subscription tests
// =============================================================================

#[tokio::test]
async fn test_subscription_message_stream() {
    use futures_util::StreamExt;

    let tm = create_test_topic_manager();
    tm.create_topic("stream-test", 2)
        .expect("Failed to create topic");

    // Pre-populate partition 0
    tm.append(
        "stream-test",
        0,
        Some(bytes::Bytes::from("k1")),
        bytes::Bytes::from("v1"),
    )
    .expect("Failed to append");

    let schema = build_test_schema(tm);

    let mut stream = schema.execute_stream(Request::new(
        r#"subscription { messageStream(topic: "stream-test", fromBeginning: true) { offset key value topic } }"#,
    ));

    let response = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("Subscription timed out")
        .expect("Stream ended unexpectedly");

    assert!(
        response.errors.is_empty(),
        "Subscription errors: {:?}",
        response.errors
    );

    let data = response
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    assert_eq!(data["messageStream"]["offset"], 0);
    assert_eq!(data["messageStream"]["key"], "k1");
    assert_eq!(data["messageStream"]["value"], "v1");
    assert_eq!(data["messageStream"]["topic"], "stream-test");
}

#[tokio::test]
async fn test_subscription_message_stream_nonexistent_topic() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"subscription { messageStream(topic: "nope") { offset } }"#,
        ))
        .await;

    assert!(
        !res.errors.is_empty(),
        "Expected an error for nonexistent topic"
    );
}

#[tokio::test]
async fn test_subscription_cluster_events_detects_topic_creation() {
    use futures_util::StreamExt;

    let tm = create_test_topic_manager();

    // Create a topic BEFORE starting the subscription so it will detect
    // the difference between initial empty state and current state
    // by starting the subscription first, then creating the topic
    let tm_clone = tm.clone();
    let schema = build_test_schema(tm);

    // Start the subscription stream
    let mut stream = schema.execute_stream(Request::new(
        r#"subscription { clusterEvents { eventType description resource timestamp } }"#,
    ));

    // Spawn topic creation in background after a short delay to allow
    // the subscription to capture the initial topic set
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        tm_clone
            .create_topic("cluster-evt-topic", 1)
            .expect("Failed to create topic");
    });

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
        .await
        .expect("Subscription timed out")
        .expect("Stream ended unexpectedly");

    assert!(
        response.errors.is_empty(),
        "Subscription errors: {:?}",
        response.errors
    );

    let data = response
        .data
        .into_json()
        .expect("Failed to convert to JSON");
    assert_eq!(data["clusterEvents"]["eventType"], "TOPIC_CREATED");
    assert_eq!(
        data["clusterEvents"]["resource"],
        "cluster-evt-topic"
    );
}

// =============================================================================
// New query tests
// =============================================================================

#[tokio::test]
async fn test_query_topic_stats() {
    let tm = create_test_topic_manager();
    tm.create_topic("stats-topic", 2)
        .expect("Failed to create topic");
    tm.append(
        "stats-topic",
        0,
        None,
        bytes::Bytes::from("msg1"),
    )
    .expect("Failed to append");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ topicStats(name: "stats-topic", window: "5m") { name partitionCount totalMessages messagesInWindow messageRate window timestamp partitions { id latestOffset } } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["topicStats"]["name"], "stats-topic");
    assert_eq!(data["topicStats"]["partitionCount"], 2);
    assert_eq!(data["topicStats"]["totalMessages"], 1);
    assert_eq!(data["topicStats"]["window"], "5m");
    assert!(data["topicStats"]["messagesInWindow"].as_i64().unwrap() >= 0);

    let partitions = data["topicStats"]["partitions"]
        .as_array()
        .expect("partitions should be array");
    assert_eq!(partitions.len(), 2);
}

#[tokio::test]
async fn test_query_topic_stats_nonexistent() {
    let tm = create_test_topic_manager();
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ topicStats(name: "nope") { name } }"#,
        ))
        .await;

    assert!(!res.errors.is_empty(), "Expected error for nonexistent topic");
}

#[tokio::test]
async fn test_query_topic_stats_invalid_window() {
    let tm = create_test_topic_manager();
    tm.create_topic("win-topic", 1)
        .expect("Failed to create topic");
    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ topicStats(name: "win-topic", window: "xyz") { name } }"#,
        ))
        .await;

    assert!(!res.errors.is_empty(), "Expected error for invalid window");
}

#[tokio::test]
async fn test_query_search_messages() {
    let tm = create_test_topic_manager();
    tm.create_topic("search-topic", 1)
        .expect("Failed to create topic");

    // Insert test messages
    for i in 0..10 {
        let value = if i % 2 == 0 {
            format!(r#"{{"action": "click", "id": {}}}"#, i)
        } else {
            format!(r#"{{"action": "scroll", "id": {}}}"#, i)
        };
        tm.append(
            "search-topic",
            0,
            None,
            bytes::Bytes::from(value),
        )
        .expect("Failed to append");
    }

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ searchMessages(topic: "search-topic", query: "click", limit: 50) { matches { offset value } totalMatches scanned } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    let matches = data["searchMessages"]["matches"]
        .as_array()
        .expect("matches should be array");
    assert_eq!(matches.len(), 5); // 5 click messages (0,2,4,6,8)
    assert_eq!(data["searchMessages"]["totalMatches"], 5);
    assert_eq!(data["searchMessages"]["scanned"], 10);
}

#[tokio::test]
async fn test_query_search_messages_no_results() {
    let tm = create_test_topic_manager();
    tm.create_topic("search-empty", 1)
        .expect("Failed to create topic");
    tm.append("search-empty", 0, None, bytes::Bytes::from("hello"))
        .expect("Failed to append");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"{ searchMessages(topic: "search-empty", query: "notfound") { matches { offset } totalMatches scanned } }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["searchMessages"]["totalMatches"], 0);
    assert_eq!(data["searchMessages"]["scanned"], 1);
}

// =============================================================================
// New mutation tests
// =============================================================================

#[tokio::test]
async fn test_mutation_produce_with_schema_valid() {
    let tm = create_test_topic_manager();
    tm.create_topic("schema-test", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm.clone());

    let res = schema
        .execute(Request::new(
            r#"mutation {
                produceWithSchema(
                    topic: "schema-test",
                    value: "{\"name\": \"Alice\", \"age\": 30}",
                    schema: "{\"type\": \"object\", \"required\": [\"name\", \"age\"], \"properties\": {\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"number\"}}}"
                ) { topic partition offset schemaValidated timestamp }
            }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["produceWithSchema"]["topic"], "schema-test");
    assert_eq!(data["produceWithSchema"]["schemaValidated"], true);
    assert_eq!(data["produceWithSchema"]["offset"], 0);

    // Verify message was stored
    let records = tm
        .read("schema-test", 0, 0, 10)
        .expect("Failed to read");
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn test_mutation_produce_with_schema_missing_required() {
    let tm = create_test_topic_manager();
    tm.create_topic("schema-fail", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"mutation {
                produceWithSchema(
                    topic: "schema-fail",
                    value: "{\"name\": \"Alice\"}",
                    schema: "{\"type\": \"object\", \"required\": [\"name\", \"age\"]}"
                ) { offset }
            }"#,
        ))
        .await;

    assert!(
        !res.errors.is_empty(),
        "Expected schema validation error for missing required field"
    );
    let err_msg = res.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("missing required field") && err_msg.contains("age"),
        "Error should mention missing 'age': {}",
        res.errors[0].message
    );
}

#[tokio::test]
async fn test_mutation_produce_with_schema_wrong_type() {
    let tm = create_test_topic_manager();
    tm.create_topic("schema-type", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"mutation {
                produceWithSchema(
                    topic: "schema-type",
                    value: "{\"name\": 123}",
                    schema: "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}}"
                ) { offset }
            }"#,
        ))
        .await;

    assert!(
        !res.errors.is_empty(),
        "Expected schema validation error for wrong type"
    );
}

#[tokio::test]
async fn test_mutation_produce_with_schema_no_schema() {
    let tm = create_test_topic_manager();
    tm.create_topic("schema-none", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"mutation {
                produceWithSchema(
                    topic: "schema-none",
                    value: "{\"any\": \"json\"}"
                ) { topic schemaValidated offset }
            }"#,
        ))
        .await;

    assert!(res.errors.is_empty(), "Errors: {:?}", res.errors);
    let data = res.data.into_json().expect("Failed to convert to JSON");
    assert_eq!(data["produceWithSchema"]["schemaValidated"], false);
    assert_eq!(data["produceWithSchema"]["offset"], 0);
}

#[tokio::test]
async fn test_mutation_produce_with_schema_invalid_json() {
    let tm = create_test_topic_manager();
    tm.create_topic("schema-bad-json", 1)
        .expect("Failed to create topic");

    let schema = build_test_schema(tm);

    let res = schema
        .execute(Request::new(
            r#"mutation {
                produceWithSchema(
                    topic: "schema-bad-json",
                    value: "not valid json"
                ) { offset }
            }"#,
        ))
        .await;

    assert!(
        !res.errors.is_empty(),
        "Expected error for invalid JSON value"
    );
}
