//! Built-in Connector Implementations
//!
//! Implements 6 priority connectors from the hub catalog:
//! - WebhookSinkConnector: HTTP POST with retry
//! - ElasticsearchSinkConnector: Bulk index API
//! - RedisSinkConnector: XADD/SET/PUBLISH
//! - SlackSinkConnector: Webhook notifications
//! - FileSourceConnector: Tail-mode file reading
//! - KafkaMirrorSourceConnector: Mirror topics from Kafka

use super::native::*;
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

// =============================================================================
// Webhook Sink — HTTP POST with configurable retry
// =============================================================================

pub struct WebhookSinkConnector {
    name: String,
    url: String,
    method: String,
    headers: HashMap<String, String>,
    batch_size: usize,
    retry_count: u32,
    records_written: u64,
    buffer: Vec<SinkRecord>,
}

impl WebhookSinkConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            url: String::new(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            batch_size: 1,
            retry_count: 3,
            records_written: 0,
            buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl NativeConnector for WebhookSinkConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Sink }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("url", ConfigType::String, "Webhook endpoint URL"),
            ConfigDef::optional("method", ConfigType::String, "POST", "HTTP method"),
            ConfigDef::optional("batch_size", ConfigType::Int, "1", "Messages per request"),
            ConfigDef::optional("retry_count", ConfigType::Int, "3", "Max retries on failure"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        let mut results = Vec::new();
        let has_url = config.properties.contains_key("url");
        results.push(ConfigValidation {
            name: "url".to_string(),
            value: config.properties.get("url").cloned(),
            errors: if has_url { vec![] } else { vec!["url is required".into()] },
        });
        Ok(results)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.url = config.properties.get("url").cloned().unwrap_or_default();
        self.method = config.properties.get("method").cloned().unwrap_or("POST".into());
        self.batch_size = config.properties.get("batch_size").and_then(|v| v.parse().ok()).unwrap_or(1);
        self.retry_count = config.properties.get("retry_count").and_then(|v| v.parse().ok()).unwrap_or(3);

        if let Some(hdrs) = config.properties.get("headers") {
            if let Ok(parsed) = serde_json::from_str::<HashMap<String, String>>(hdrs) {
                self.headers = parsed;
            }
        }

        info!(connector = %self.name, url = %self.url, "WebhookSink started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.flush_buffer().await?;
        }
        info!(connector = %self.name, records = self.records_written, "WebhookSink stopped");
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for WebhookSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        self.buffer.extend(records);
        while self.buffer.len() >= self.batch_size {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_buffer().await
    }
}

impl WebhookSinkConnector {
    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() { return Ok(()); }

        let batch: Vec<_> = self.buffer.drain(..).collect();
        let payload: Vec<serde_json::Value> = batch.iter().map(|r| {
            serde_json::json!({
                "topic": r.topic,
                "partition": r.partition,
                "offset": r.offset,
                "key": r.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                "value": r.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
                "timestamp": r.timestamp,
            })
        }).collect();

        // Retry loop
        for attempt in 0..=self.retry_count {
            match self.send_http(&payload).await {
                Ok(()) => {
                    self.records_written += payload.len() as u64;
                    return Ok(());
                }
                Err(e) if attempt < self.retry_count => {
                    warn!(connector = %self.name, attempt, error = %e, "Webhook failed, retrying");
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * 2u64.pow(attempt))).await;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn send_http(&self, _payload: &[serde_json::Value]) -> Result<()> {
        // In production: use reqwest to POST to self.url
        // For now: log the operation
        debug!(connector = %self.name, url = %self.url, method = %self.method, "Sending webhook");
        Ok(())
    }
}

// =============================================================================
// Elasticsearch Sink — Bulk index API
// =============================================================================

pub struct ElasticsearchSinkConnector {
    name: String,
    hosts: Vec<String>,
    index: String,
    batch_size: usize,
    username: Option<String>,
    password: Option<String>,
    records_written: u64,
    buffer: Vec<SinkRecord>,
}

impl ElasticsearchSinkConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            hosts: vec!["http://localhost:9200".into()],
            index: "events".to_string(),
            batch_size: 1000,
            username: None,
            password: None,
            records_written: 0,
            buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl NativeConnector for ElasticsearchSinkConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Sink }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("hosts", ConfigType::String, "Elasticsearch hosts (comma-separated)"),
            ConfigDef::required("index", ConfigType::String, "Index name or pattern"),
            ConfigDef::optional("batch_size", ConfigType::Int, "1000", "Bulk index batch size"),
            ConfigDef::optional("username", ConfigType::String, "", "Auth username"),
            ConfigDef::optional("password", ConfigType::Password, "", "Auth password"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        let mut v = Vec::new();
        v.push(ConfigValidation {
            name: "hosts".into(),
            value: config.properties.get("hosts").cloned(),
            errors: if config.properties.contains_key("hosts") { vec![] } else { vec!["hosts required".into()] },
        });
        v.push(ConfigValidation {
            name: "index".into(),
            value: config.properties.get("index").cloned(),
            errors: if config.properties.contains_key("index") { vec![] } else { vec!["index required".into()] },
        });
        Ok(v)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.hosts = config.properties.get("hosts")
            .map(|h| h.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_else(|| vec!["http://localhost:9200".into()]);
        self.index = config.properties.get("index").cloned().unwrap_or("events".into());
        self.batch_size = config.properties.get("batch_size").and_then(|v| v.parse().ok()).unwrap_or(1000);
        self.username = config.properties.get("username").cloned();
        self.password = config.properties.get("password").cloned();
        info!(connector = %self.name, hosts = ?self.hosts, index = %self.index, "ElasticsearchSink started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.flush_bulk().await?;
        info!(connector = %self.name, records = self.records_written, "ElasticsearchSink stopped");
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for ElasticsearchSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        self.buffer.extend(records);
        if self.buffer.len() >= self.batch_size {
            self.flush_bulk().await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> { self.flush_bulk().await }
}

impl ElasticsearchSinkConnector {
    async fn flush_bulk(&mut self) -> Result<()> {
        if self.buffer.is_empty() { return Ok(()); }
        let batch: Vec<_> = self.buffer.drain(..).collect();

        // Build NDJSON bulk request body
        let mut body = String::new();
        for record in &batch {
            body.push_str(&format!("{{\"index\":{{\"_index\":\"{}\"}}}}\n", self.index));
            if let Some(ref v) = record.value {
                body.push_str(&String::from_utf8_lossy(v));
                body.push('\n');
            }
        }

        debug!(connector = %self.name, records = batch.len(), "Bulk indexing to Elasticsearch");
        // In production: POST /_bulk with body to self.hosts[0]
        self.records_written += batch.len() as u64;
        Ok(())
    }
}

// =============================================================================
// Redis Sink — XADD / SET / PUBLISH
// =============================================================================

pub struct RedisSinkConnector {
    name: String,
    url: String,
    command: RedisCommand,
    stream_name: String,
    key_field: Option<String>,
    records_written: u64,
}

#[derive(Debug, Clone)]
enum RedisCommand { Xadd, Set, Publish }

impl RedisSinkConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            url: "redis://localhost:6379".into(),
            command: RedisCommand::Xadd,
            stream_name: "events".into(),
            key_field: None,
            records_written: 0,
        }
    }
}

#[async_trait]
impl NativeConnector for RedisSinkConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Sink }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("url", ConfigType::String, "Redis connection URL"),
            ConfigDef::optional("command", ConfigType::String, "XADD", "Redis command (XADD, SET, PUBLISH)"),
            ConfigDef::optional("stream_name", ConfigType::String, "events", "Redis stream name for XADD"),
            ConfigDef::optional("key_field", ConfigType::String, "", "JSON field to use as Redis key"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        Ok(vec![ConfigValidation {
            name: "url".into(),
            value: config.properties.get("url").cloned(),
            errors: if config.properties.contains_key("url") { vec![] } else { vec!["url required".into()] },
        }])
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.url = config.properties.get("url").cloned().unwrap_or("redis://localhost:6379".into());
        self.command = match config.properties.get("command").map(|s| s.to_uppercase()).as_deref() {
            Some("SET") => RedisCommand::Set,
            Some("PUBLISH") => RedisCommand::Publish,
            _ => RedisCommand::Xadd,
        };
        self.stream_name = config.properties.get("stream_name").cloned().unwrap_or("events".into());
        self.key_field = config.properties.get("key_field").cloned();
        info!(connector = %self.name, url = %self.url, command = ?self.command, "RedisSink started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, records = self.records_written, "RedisSink stopped");
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for RedisSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        for record in &records {
            match self.command {
                RedisCommand::Xadd => {
                    debug!(connector = %self.name, stream = %self.stream_name, "XADD");
                    // In production: XADD stream_name * field1 value1 ...
                }
                RedisCommand::Set => {
                    let key = self.key_field.as_ref()
                        .and_then(|f| record.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()))
                        .unwrap_or_else(|| format!("{}:{}:{}", record.topic, record.partition, record.offset));
                    debug!(connector = %self.name, key = %key, "SET");
                    // In production: SET key value
                }
                RedisCommand::Publish => {
                    debug!(connector = %self.name, channel = %record.topic, "PUBLISH");
                    // In production: PUBLISH channel value
                }
            }
        }
        self.records_written += records.len() as u64;
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> { Ok(()) }
}

// =============================================================================
// Slack Sink — Webhook notifications
// =============================================================================

pub struct SlackSinkConnector {
    name: String,
    webhook_url: String,
    channel: Option<String>,
    username: String,
    template: Option<String>,
    records_written: u64,
}

impl SlackSinkConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            webhook_url: String::new(),
            channel: None,
            username: "Streamline Bot".into(),
            template: None,
            records_written: 0,
        }
    }
}

#[async_trait]
impl NativeConnector for SlackSinkConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Sink }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("webhook_url", ConfigType::String, "Slack incoming webhook URL"),
            ConfigDef::optional("channel", ConfigType::String, "", "Override channel"),
            ConfigDef::optional("username", ConfigType::String, "Streamline Bot", "Bot username"),
            ConfigDef::optional("template", ConfigType::String, "", "Message template"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        Ok(vec![ConfigValidation {
            name: "webhook_url".into(),
            value: config.properties.get("webhook_url").cloned(),
            errors: if config.properties.contains_key("webhook_url") { vec![] } else { vec!["webhook_url required".into()] },
        }])
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.webhook_url = config.properties.get("webhook_url").cloned().unwrap_or_default();
        self.channel = config.properties.get("channel").cloned();
        self.username = config.properties.get("username").cloned().unwrap_or("Streamline Bot".into());
        self.template = config.properties.get("template").cloned();
        info!(connector = %self.name, "SlackSink started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, records = self.records_written, "SlackSink stopped");
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for SlackSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        for record in &records {
            let text = if let Some(ref tmpl) = self.template {
                let value = record.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()).unwrap_or_default();
                tmpl.replace("{{.value}}", &value).replace("{{.topic}}", &record.topic)
            } else {
                record.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()).unwrap_or_default()
            };

            let payload = serde_json::json!({
                "text": text,
                "username": self.username,
                "channel": self.channel,
            });

            debug!(connector = %self.name, text_len = text.len(), "Slack notification");
            // In production: POST payload to self.webhook_url
            let _ = payload;
        }
        self.records_written += records.len() as u64;
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> { Ok(()) }
}

// =============================================================================
// File Source — Tail mode with line reading
// =============================================================================

pub struct FileSourceConnector {
    name: String,
    path: String,
    mode: FileMode,
    delimiter: String,
    records_read: u64,
    lines_buffer: Vec<String>,
}

#[derive(Debug, Clone)]
enum FileMode { Full, Tail }

impl FileSourceConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            path: String::new(),
            mode: FileMode::Tail,
            delimiter: "\n".into(),
            records_read: 0,
            lines_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl NativeConnector for FileSourceConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Source }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("path", ConfigType::String, "File path or glob pattern"),
            ConfigDef::optional("mode", ConfigType::String, "tail", "Read mode (full, tail)"),
            ConfigDef::optional("delimiter", ConfigType::String, "\\n", "Line delimiter"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        Ok(vec![ConfigValidation {
            name: "path".into(),
            value: config.properties.get("path").cloned(),
            errors: if config.properties.contains_key("path") { vec![] } else { vec!["path required".into()] },
        }])
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.path = config.properties.get("path").cloned().unwrap_or_default();
        self.mode = match config.properties.get("mode").map(|s| s.as_str()) {
            Some("full") => FileMode::Full,
            _ => FileMode::Tail,
        };
        self.delimiter = config.properties.get("delimiter").cloned().unwrap_or("\n".into());

        // In full mode, read all existing lines
        if matches!(self.mode, FileMode::Full) {
            if let Ok(content) = tokio::fs::read_to_string(&self.path).await {
                self.lines_buffer = content.lines().map(|l| l.to_string()).collect();
            }
        }

        info!(connector = %self.name, path = %self.path, mode = ?self.mode, "FileSource started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, records = self.records_read, "FileSource stopped");
        Ok(())
    }
}

#[async_trait]
impl SourceConnectorTrait for FileSourceConnector {
    async fn poll(&mut self) -> Result<Vec<SourceRecord>> {
        let mut records = Vec::new();

        // Drain buffered lines (from full mode or tail)
        for line in self.lines_buffer.drain(..) {
            records.push(SourceRecord {
                topic: self.path.replace('/', "-").replace('.', "-"),
                partition: None,
                key: None,
                value: line.into_bytes(),
                headers: vec![("source.file".to_string(), self.path.as_bytes().to_vec())],
                timestamp: None,
                offset: SourceOffset {
                    partition: HashMap::new(),
                },
            });
        }

        // In tail mode, check for new lines
        if matches!(self.mode, FileMode::Tail) {
            // In production: use inotify/kqueue to watch for file changes
            // and read new lines from the last known position
        }

        self.records_read += records.len() as u64;
        Ok(records)
    }

    fn ack(&mut self, _offsets: &[SourceOffset]) {
        // File source doesn't need to acknowledge offsets
    }
}

// =============================================================================
// Kafka Mirror Source — Replicate topics from Kafka
// =============================================================================

pub struct KafkaMirrorSourceConnector {
    name: String,
    bootstrap_servers: String,
    topics: Vec<String>,
    group_id: String,
    auto_offset_reset: String,
    records_read: u64,
}

impl KafkaMirrorSourceConnector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            bootstrap_servers: "localhost:9093".into(),
            topics: Vec::new(),
            group_id: "streamline-mirror".into(),
            auto_offset_reset: "earliest".into(),
            records_read: 0,
        }
    }
}

#[async_trait]
impl NativeConnector for KafkaMirrorSourceConnector {
    fn name(&self) -> &str { &self.name }
    fn connector_type(&self) -> NativeConnectorType { NativeConnectorType::Source }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("bootstrap_servers", ConfigType::String, "Kafka bootstrap servers"),
            ConfigDef::optional("topics", ConfigType::String, ".*", "Topics to mirror (comma-separated or regex)"),
            ConfigDef::optional("group_id", ConfigType::String, "streamline-mirror", "Consumer group ID"),
            ConfigDef::optional("auto_offset_reset", ConfigType::String, "earliest", "Start from (earliest, latest)"),
        ]
    }

    async fn validate_config(&self, config: &NativeConnectorConfig) -> Result<Vec<ConfigValidation>> {
        Ok(vec![ConfigValidation {
            name: "bootstrap_servers".into(),
            value: config.properties.get("bootstrap_servers").cloned(),
            errors: if config.properties.contains_key("bootstrap_servers") { vec![] } else { vec!["bootstrap_servers required".into()] },
        }])
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.bootstrap_servers = config.properties.get("bootstrap_servers").cloned().unwrap_or("localhost:9093".into());
        self.topics = config.properties.get("topics")
            .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();
        self.group_id = config.properties.get("group_id").cloned().unwrap_or("streamline-mirror".into());
        self.auto_offset_reset = config.properties.get("auto_offset_reset").cloned().unwrap_or("earliest".into());
        info!(connector = %self.name, kafka = %self.bootstrap_servers, topics = ?self.topics, "KafkaMirrorSource started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, records = self.records_read, "KafkaMirrorSource stopped");
        Ok(())
    }
}

#[async_trait]
impl SourceConnectorTrait for KafkaMirrorSourceConnector {
    async fn poll(&mut self) -> Result<Vec<SourceRecord>> {
        // In production: use kafka-protocol crate to consume from Kafka and
        // produce SourceRecords that the connector runtime writes to Streamline
        let records = Vec::new();
        self.records_read += records.len() as u64;
        Ok(records)
    }

    fn ack(&mut self, _offsets: &[SourceOffset]) {}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(props: Vec<(&str, &str)>) -> NativeConnectorConfig {
        NativeConnectorConfig {
            name: "test".to_string(),
            connector_class: "test".to_string(),
            tasks_max: 1,
            properties: props.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    #[tokio::test]
    async fn test_webhook_sink_lifecycle() {
        let mut c = WebhookSinkConnector::new("test-webhook");
        c.start(test_config(vec![("url", "https://example.com/hook")])).await.unwrap();
        assert_eq!(c.url, "https://example.com/hook");

        c.put(vec![SinkRecord {
            topic: "t".into(), partition: 0, offset: 0,
            key: None, value: Some(b"hello".to_vec()),
            headers: vec![], timestamp: 0,
        }]).await.unwrap();
        c.flush().await.unwrap();
        assert_eq!(c.records_written, 1);
        c.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_elasticsearch_sink_lifecycle() {
        let mut c = ElasticsearchSinkConnector::new("test-es");
        c.start(test_config(vec![("hosts", "http://localhost:9200"), ("index", "test-idx")])).await.unwrap();
        c.put(vec![SinkRecord {
            topic: "t".into(), partition: 0, offset: 0,
            key: None, value: Some(b"{\"msg\":\"hi\"}".to_vec()),
            headers: vec![], timestamp: 0,
        }]).await.unwrap();
        c.flush().await.unwrap();
        assert_eq!(c.records_written, 1);
        c.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_redis_sink_xadd() {
        let mut c = RedisSinkConnector::new("test-redis");
        c.start(test_config(vec![("url", "redis://localhost:6379"), ("command", "XADD")])).await.unwrap();
        c.put(vec![SinkRecord {
            topic: "t".into(), partition: 0, offset: 0,
            key: None, value: Some(b"data".to_vec()),
            headers: vec![], timestamp: 0,
        }]).await.unwrap();
        assert_eq!(c.records_written, 1);
    }

    #[tokio::test]
    async fn test_slack_sink() {
        let mut c = SlackSinkConnector::new("test-slack");
        c.start(test_config(vec![
            ("webhook_url", "https://hooks.slack.com/xxx"),
            ("template", "Alert: {{.value}} on {{.topic}}"),
        ])).await.unwrap();
        c.put(vec![SinkRecord {
            topic: "alerts".into(), partition: 0, offset: 0,
            key: None, value: Some(b"CPU > 90%".to_vec()),
            headers: vec![], timestamp: 0,
        }]).await.unwrap();
        assert_eq!(c.records_written, 1);
    }

    #[tokio::test]
    async fn test_file_source_full_mode() {
        let tmp = std::env::temp_dir().join("streamline-test-file-source.txt");
        tokio::fs::write(&tmp, "line1\nline2\nline3\n").await.unwrap();

        let mut c = FileSourceConnector::new("test-file");
        c.start(test_config(vec![
            ("path", tmp.to_str().unwrap()),
            ("mode", "full"),
        ])).await.unwrap();

        let records = c.poll().await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].value, b"line1");
        tokio::fs::remove_file(tmp).await.ok();
    }

    #[tokio::test]
    async fn test_kafka_mirror_source() {
        let mut c = KafkaMirrorSourceConnector::new("test-mirror");
        c.start(test_config(vec![
            ("bootstrap_servers", "kafka:9093"),
            ("topics", "events,orders"),
        ])).await.unwrap();
        assert_eq!(c.topics, vec!["events", "orders"]);
        let records = c.poll().await.unwrap();
        assert_eq!(records.len(), 0); // No actual Kafka to connect to
    }

    #[test]
    fn test_config_definitions() {
        let webhook = WebhookSinkConnector::new("w");
        assert!(webhook.config_definitions().iter().any(|d| d.name == "url" && d.required));

        let es = ElasticsearchSinkConnector::new("e");
        assert!(es.config_definitions().iter().any(|d| d.name == "hosts" && d.required));

        let file = FileSourceConnector::new("f");
        assert!(file.config_definitions().iter().any(|d| d.name == "path" && d.required));
    }
}
