//! Migration Validator - Verify migration completeness and correctness
//!
//! This module provides tools to validate that a migration from Kafka to
//! Streamline was successful by comparing topic configurations, message
//! counts, and consumer lag.

use bytes::{BufMut, BytesMut};
use colored::Colorize;
use kafka_protocol::messages::{
    ApiKey, BrokerId, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::HashMap;
use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::time::Duration;

/// Configuration for migration validation
#[derive(Debug, Clone)]
pub struct ValidatorConfig {
    /// Kafka bootstrap servers
    pub kafka_bootstrap_servers: String,
    /// Streamline bootstrap servers
    pub streamline_bootstrap_servers: String,
    /// Topics to validate (empty = all)
    pub topics: Vec<String>,
    /// Whether to compare message counts
    pub compare_counts: bool,
    /// Whether to validate consumer groups
    pub validate_groups: bool,
    /// Output format (text, json)
    pub output_format: String,
    /// Output file
    pub output_file: Option<std::path::PathBuf>,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            kafka_bootstrap_servers: "localhost:9092".into(),
            streamline_bootstrap_servers: "localhost:9093".into(),
            topics: vec![],
            compare_counts: true,
            validate_groups: true,
            output_format: "text".into(),
            output_file: None,
        }
    }
}

/// Validation check result
#[derive(Debug, Clone)]
pub enum CheckStatus {
    Pass,
    Warn(String),
    Fail(String),
}

impl CheckStatus {
    pub fn is_pass(&self) -> bool {
        matches!(self, CheckStatus::Pass)
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, CheckStatus::Fail(_))
    }
}

/// Individual validation check
#[derive(Debug, Clone)]
pub struct ValidationCheck {
    pub name: String,
    pub category: String,
    pub status: CheckStatus,
    pub details: Option<String>,
}

/// Validation result
#[derive(Debug)]
pub struct ValidationResult {
    pub checks: Vec<ValidationCheck>,
    pub topics_validated: usize,
    pub groups_validated: usize,
    pub passed: usize,
    pub warnings: usize,
    pub failed: usize,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.failed == 0
    }
}

/// Topic comparison info
#[allow(dead_code)]
#[derive(Debug)]
struct TopicComparison {
    name: String,
    kafka_partitions: i32,
    streamline_partitions: i32,
    kafka_messages: i64,
    streamline_messages: i64,
}

/// Kafka client for validation
struct ValidatorKafkaClient {
    stream: TcpStream,
    correlation_id: i32,
    client_id: String,
}

impl ValidatorKafkaClient {
    fn connect(addr: &str) -> crate::Result<Self> {
        let addr = addr
            .parse()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092)));

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
            .map_err(crate::StreamlineError::Io)?;

        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(10))).ok();

        Ok(Self {
            stream,
            correlation_id: 0,
            client_id: "streamline-validator".to_string(),
        })
    }

    fn send_request<Req: Encodable, Resp: Decodable>(
        &mut self,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> crate::Result<Resp> {
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_string(self.client_id.clone())));

        let mut buf = BytesMut::new();
        header
            .encode(&mut buf, api_version)
            .map_err(|e| crate::StreamlineError::protocol_msg(format!("encode header: {}", e)))?;
        request
            .encode(&mut buf, api_version)
            .map_err(|e| crate::StreamlineError::protocol_msg(format!("encode request: {}", e)))?;

        let len = buf.len() as i32;
        let mut msg = BytesMut::with_capacity(4 + buf.len());
        msg.put_i32(len);
        msg.extend_from_slice(&buf);

        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let resp_len = i32::from_be_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp_buf)?;

        let mut cursor = &resp_buf[..];
        let _resp_header = ResponseHeader::decode(&mut cursor, api_version)
            .map_err(|e| crate::StreamlineError::protocol_msg(format!("decode header: {}", e)))?;
        let response = Resp::decode(&mut cursor, api_version)
            .map_err(|e| crate::StreamlineError::protocol_msg(format!("decode response: {}", e)))?;

        Ok(response)
    }

    fn metadata(&mut self, topics: Option<&[String]>) -> crate::Result<MetadataResponse> {
        use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

        let request = if let Some(topic_names) = topics {
            let topics: Vec<MetadataRequestTopic> = topic_names
                .iter()
                .map(|name| {
                    MetadataRequestTopic::default()
                        .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                })
                .collect();
            MetadataRequest::default()
                .with_topics(Some(topics))
                .with_allow_auto_topic_creation(false)
        } else {
            MetadataRequest::default()
                .with_topics(Some(vec![]))
                .with_allow_auto_topic_creation(false)
        };

        self.send_request(ApiKey::Metadata, 9, &request)
    }

    fn get_log_end_offsets(
        &mut self,
        topics: &[(String, i32)],
    ) -> crate::Result<HashMap<(String, i32), i64>> {
        use kafka_protocol::messages::list_offsets_request::{
            ListOffsetsPartition, ListOffsetsTopic,
        };

        // Group by topic
        let mut topic_map: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in topics {
            topic_map.entry(topic.clone()).or_default().push(*partition);
        }

        let topics: Vec<ListOffsetsTopic> = topic_map
            .into_iter()
            .map(|(name, partitions)| {
                let partition_reqs: Vec<ListOffsetsPartition> = partitions
                    .into_iter()
                    .map(|p| {
                        ListOffsetsPartition::default()
                            .with_partition_index(p)
                            .with_timestamp(-1) // Latest offset
                    })
                    .collect();
                ListOffsetsTopic::default()
                    .with_name(TopicName(StrBytes::from_string(name)))
                    .with_partitions(partition_reqs)
            })
            .collect();

        let request = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_isolation_level(0)
            .with_topics(topics);

        let response: ListOffsetsResponse = self.send_request(ApiKey::ListOffsets, 5, &request)?;

        let mut offsets = HashMap::new();
        for topic in &response.topics {
            let topic_name = topic.name.as_str().to_string();
            for partition in &topic.partitions {
                offsets.insert(
                    (topic_name.clone(), partition.partition_index),
                    partition.offset,
                );
            }
        }

        Ok(offsets)
    }
}

/// Run the migration validation
pub fn run_validation(config: &ValidatorConfig) -> crate::Result<ValidationResult> {
    print_banner();

    let mut result = ValidationResult {
        checks: vec![],
        topics_validated: 0,
        groups_validated: 0,
        passed: 0,
        warnings: 0,
        failed: 0,
    };

    // Step 1: Connect to both clusters
    println!();
    print_step(1, "Connecting to clusters...");

    let kafka_addr = config
        .kafka_bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("localhost:9092");

    let streamline_addr = config
        .streamline_bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("localhost:9093");

    let mut kafka_client = match ValidatorKafkaClient::connect(kafka_addr) {
        Ok(client) => {
            println!(
                "  {} Connected to Kafka at {}",
                "✓".green(),
                kafka_addr.cyan()
            );
            add_check(
                &mut result,
                "Kafka Connection",
                "connectivity",
                CheckStatus::Pass,
                None,
            );
            client
        }
        Err(e) => {
            println!("  {} Failed to connect to Kafka: {}", "✗".red(), e);
            add_check(
                &mut result,
                "Kafka Connection",
                "connectivity",
                CheckStatus::Fail(e.to_string()),
                None,
            );
            return Ok(result);
        }
    };

    let mut streamline_client = match ValidatorKafkaClient::connect(streamline_addr) {
        Ok(client) => {
            println!(
                "  {} Connected to Streamline at {}",
                "✓".green(),
                streamline_addr.cyan()
            );
            add_check(
                &mut result,
                "Streamline Connection",
                "connectivity",
                CheckStatus::Pass,
                None,
            );
            client
        }
        Err(e) => {
            println!("  {} Failed to connect to Streamline: {}", "✗".red(), e);
            add_check(
                &mut result,
                "Streamline Connection",
                "connectivity",
                CheckStatus::Fail(e.to_string()),
                None,
            );
            return Ok(result);
        }
    };

    // Step 2: Compare topics
    println!();
    print_step(2, "Comparing topics...");

    let kafka_metadata = kafka_client.metadata(if config.topics.is_empty() {
        None
    } else {
        Some(&config.topics)
    })?;

    let streamline_metadata = streamline_client.metadata(if config.topics.is_empty() {
        None
    } else {
        Some(&config.topics)
    })?;

    // Build topic maps
    let kafka_topics: HashMap<String, &_> = kafka_metadata
        .topics
        .iter()
        .filter(|t| {
            t.name
                .as_ref()
                .map(|n| !n.as_str().starts_with("__"))
                .unwrap_or(false)
        })
        .filter_map(|t| t.name.as_ref().map(|n| (n.as_str().to_string(), t)))
        .collect();

    let streamline_topics: HashMap<String, &_> = streamline_metadata
        .topics
        .iter()
        .filter(|t| {
            t.name
                .as_ref()
                .map(|n| !n.as_str().starts_with("__"))
                .unwrap_or(false)
        })
        .filter_map(|t| t.name.as_ref().map(|n| (n.as_str().to_string(), t)))
        .collect();

    // Check for missing topics
    for topic_name in kafka_topics.keys() {
        if !streamline_topics.contains_key(topic_name) {
            println!(
                "  {} Topic '{}' missing in Streamline",
                "✗".red(),
                topic_name
            );
            add_check(
                &mut result,
                &format!("Topic '{}' exists", topic_name),
                "topics",
                CheckStatus::Fail("Missing in Streamline".to_string()),
                None,
            );
        } else {
            let kafka_topic = kafka_topics[topic_name];
            let streamline_topic = streamline_topics[topic_name];

            let kafka_partitions = kafka_topic.partitions.len() as i32;
            let streamline_partitions = streamline_topic.partitions.len() as i32;

            if kafka_partitions != streamline_partitions {
                println!(
                    "  {} Topic '{}' partition mismatch: {} vs {}",
                    "!".yellow(),
                    topic_name,
                    kafka_partitions,
                    streamline_partitions
                );
                add_check(
                    &mut result,
                    &format!("Topic '{}' partitions", topic_name),
                    "topics",
                    CheckStatus::Warn(format!(
                        "Kafka: {}, Streamline: {}",
                        kafka_partitions, streamline_partitions
                    )),
                    None,
                );
            } else {
                println!(
                    "  {} Topic '{}' ({} partitions)",
                    "✓".green(),
                    topic_name,
                    kafka_partitions
                );
                add_check(
                    &mut result,
                    &format!("Topic '{}' configuration", topic_name),
                    "topics",
                    CheckStatus::Pass,
                    None,
                );
            }
            result.topics_validated += 1;
        }
    }

    // Step 3: Compare message counts if enabled
    if config.compare_counts {
        println!();
        print_step(3, "Comparing message counts...");

        // Get partitions for all topics
        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for (topic_name, topic) in &kafka_topics {
            if streamline_topics.contains_key(topic_name) {
                for partition in &topic.partitions {
                    all_partitions.push((topic_name.clone(), partition.partition_index));
                }
            }
        }

        let kafka_offsets = kafka_client.get_log_end_offsets(&all_partitions)?;
        let streamline_offsets = streamline_client.get_log_end_offsets(&all_partitions)?;

        // Compare per topic
        let mut topic_totals: HashMap<String, (i64, i64)> = HashMap::new();
        for (key, kafka_offset) in &kafka_offsets {
            let streamline_offset = streamline_offsets.get(key).copied().unwrap_or(0);
            let entry = topic_totals.entry(key.0.clone()).or_insert((0, 0));
            entry.0 += kafka_offset;
            entry.1 += streamline_offset;
        }

        for (topic_name, (kafka_total, streamline_total)) in &topic_totals {
            let diff = *kafka_total - *streamline_total;
            if diff == 0 {
                println!(
                    "  {} Topic '{}': {} messages (match)",
                    "✓".green(),
                    topic_name,
                    kafka_total
                );
                add_check(
                    &mut result,
                    &format!("Topic '{}' message count", topic_name),
                    "data",
                    CheckStatus::Pass,
                    Some(format!("{} messages", kafka_total)),
                );
            } else if diff.abs() < (*kafka_total / 100).max(10) {
                // Within 1% or 10 messages
                println!(
                    "  {} Topic '{}': {} vs {} ({} behind)",
                    "!".yellow(),
                    topic_name,
                    kafka_total,
                    streamline_total,
                    diff
                );
                add_check(
                    &mut result,
                    &format!("Topic '{}' message count", topic_name),
                    "data",
                    CheckStatus::Warn(format!("{} messages behind", diff)),
                    None,
                );
            } else {
                println!(
                    "  {} Topic '{}': {} vs {} ({} behind)",
                    "✗".red(),
                    topic_name,
                    kafka_total,
                    streamline_total,
                    diff
                );
                add_check(
                    &mut result,
                    &format!("Topic '{}' message count", topic_name),
                    "data",
                    CheckStatus::Fail(format!("{} messages behind", diff)),
                    None,
                );
            }
        }
    } else {
        println!();
        print_step(3, "Skipping message count comparison");
        println!("  {} Message count comparison disabled", "ℹ".blue());
    }

    // Step 4: Validate consumer groups if enabled
    if config.validate_groups {
        println!();
        print_step(4, "Validating consumer groups...");

        // List consumer groups on both Kafka and Streamline using ListGroups API
        use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};

        let kafka_groups: Vec<String> =
            match kafka_client.send_request::<ListGroupsRequest, ListGroupsResponse>(
                ApiKey::ListGroups,
                4,
                &ListGroupsRequest::default(),
            ) {
                Ok(resp) => resp
                    .groups
                    .iter()
                    .map(|g| g.group_id.as_str().to_string())
                    .collect(),
                Err(e) => {
                    println!("  {} Failed to list Kafka groups: {}", "!".yellow(), e);
                    vec![]
                }
            };

        let streamline_groups: Vec<String> =
            match streamline_client.send_request::<ListGroupsRequest, ListGroupsResponse>(
                ApiKey::ListGroups,
                4,
                &ListGroupsRequest::default(),
            ) {
                Ok(resp) => resp
                    .groups
                    .iter()
                    .map(|g| g.group_id.as_str().to_string())
                    .collect(),
                Err(e) => {
                    println!(
                        "  {} Failed to list Streamline groups: {}",
                        "!".yellow(),
                        e
                    );
                    vec![]
                }
            };

        // Validate that all Kafka groups exist in Streamline
        let mut groups_checked = 0usize;
        for group_id in &kafka_groups {
            groups_checked += 1;
            if streamline_groups.contains(group_id) {
                println!("  {} Group '{}' exists on both sides", "✓".green(), group_id);
                add_check(
                    &mut result,
                    &format!("Consumer group '{}'", group_id),
                    "groups",
                    CheckStatus::Pass,
                    None,
                );
            } else {
                println!(
                    "  {} Group '{}' missing in Streamline",
                    "!".yellow(),
                    group_id
                );
                add_check(
                    &mut result,
                    &format!("Consumer group '{}'", group_id),
                    "groups",
                    CheckStatus::Warn("Missing in Streamline".to_string()),
                    None,
                );
            }
        }
        result.groups_validated = groups_checked;

        println!(
            "  {} Validated {} consumer groups",
            "✓".green(),
            groups_checked
        );
    } else {
        println!();
        print_step(4, "Skipping consumer group validation");
        println!("  {} Consumer group validation disabled", "ℹ".blue());
    }

    // Step 5: Generate report
    println!();
    print_step(5, "Generating report...");

    // Count results
    result.passed = result.checks.iter().filter(|c| c.status.is_pass()).count();
    result.warnings = result
        .checks
        .iter()
        .filter(|c| matches!(c.status, CheckStatus::Warn(_)))
        .count();
    result.failed = result.checks.iter().filter(|c| c.status.is_fail()).count();

    // Output to file if requested
    if let Some(ref output_path) = config.output_file {
        let report = generate_report(&result, &config.output_format);
        std::fs::write(output_path, report)?;
        println!(
            "  {} Report written to {}",
            "✓".green(),
            output_path.display()
        );
    }

    // Summary
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!("  {}", "Validation Summary".bold().cyan());
    println!();
    println!(
        "  Topics validated: {}",
        result.topics_validated.to_string().cyan()
    );
    println!("  Checks passed:    {}", result.passed.to_string().green());
    println!(
        "  Warnings:         {}",
        if result.warnings == 0 {
            "0".normal()
        } else {
            result.warnings.to_string().yellow()
        }
    );
    println!(
        "  Failed:           {}",
        if result.failed == 0 {
            "0".green()
        } else {
            result.failed.to_string().red()
        }
    );
    println!();

    if result.is_valid() {
        println!(
            "  {} Migration validation {}",
            "✓".green().bold(),
            "PASSED".green().bold()
        );
    } else {
        println!(
            "  {} Migration validation {}",
            "✗".red().bold(),
            "FAILED".red().bold()
        );
    }

    Ok(result)
}

fn add_check(
    result: &mut ValidationResult,
    name: &str,
    category: &str,
    status: CheckStatus,
    details: Option<String>,
) {
    result.checks.push(ValidationCheck {
        name: name.to_string(),
        category: category.to_string(),
        status,
        details,
    });
}

fn generate_report(result: &ValidationResult, format: &str) -> String {
    match format {
        "json" => serde_json::to_string_pretty(&serde_json::json!({
            "topics_validated": result.topics_validated,
            "passed": result.passed,
            "warnings": result.warnings,
            "failed": result.failed,
            "valid": result.is_valid(),
            "checks": result.checks.iter().map(|c| {
                let (status_str, message) = match &c.status {
                    CheckStatus::Pass => ("pass", None),
                    CheckStatus::Warn(msg) => ("warn", Some(msg.clone())),
                    CheckStatus::Fail(msg) => ("fail", Some(msg.clone())),
                };
                serde_json::json!({
                    "name": c.name,
                    "category": c.category,
                    "status": status_str,
                    "message": message,
                    "details": c.details,
                })
            }).collect::<Vec<_>>(),
        }))
        .unwrap_or_default(),
        _ => {
            // Text format
            let mut report = String::new();
            report.push_str("# Migration Validation Report\n\n");
            report.push_str(&format!(
                "Generated: {}\n\n",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
            ));

            report.push_str("## Summary\n\n");
            report.push_str(&format!(
                "- Topics validated: {}\n",
                result.topics_validated
            ));
            report.push_str(&format!("- Checks passed: {}\n", result.passed));
            report.push_str(&format!("- Warnings: {}\n", result.warnings));
            report.push_str(&format!("- Failed: {}\n", result.failed));
            report.push_str(&format!(
                "- Status: {}\n\n",
                if result.is_valid() {
                    "PASSED"
                } else {
                    "FAILED"
                }
            ));

            report.push_str("## Checks\n\n");
            report.push_str("| Check | Category | Status | Details |\n");
            report.push_str("|-------|----------|--------|--------|\n");

            for check in &result.checks {
                let status = match &check.status {
                    CheckStatus::Pass => "✓ Pass",
                    CheckStatus::Warn(msg) => &format!("⚠ Warn: {}", msg),
                    CheckStatus::Fail(msg) => &format!("✗ Fail: {}", msg),
                };
                report.push_str(&format!(
                    "| {} | {} | {} | {} |\n",
                    check.name,
                    check.category,
                    status,
                    check.details.as_deref().unwrap_or("-")
                ));
            }

            report
        }
    }
}

fn print_banner() {
    println!();
    println!("  {}", "Streamline Migration Validator".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();
    println!(
        "  {}",
        "Verify migration completeness and correctness".dimmed()
    );
}

fn print_step(num: u32, message: &str) {
    println!(
        "  {} {}",
        format!("[{}/5]", num).cyan().bold(),
        message.bold()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_config_default() {
        let config = ValidatorConfig::default();
        assert_eq!(config.kafka_bootstrap_servers, "localhost:9092");
        assert!(config.compare_counts);
    }

    #[test]
    fn test_check_status() {
        assert!(CheckStatus::Pass.is_pass());
        assert!(!CheckStatus::Pass.is_fail());
        assert!(CheckStatus::Fail("error".to_string()).is_fail());
    }

    #[test]
    fn test_validation_result_is_valid() {
        let mut result = ValidationResult {
            checks: vec![],
            topics_validated: 0,
            groups_validated: 0,
            passed: 5,
            warnings: 2,
            failed: 0,
        };
        assert!(result.is_valid());

        result.failed = 1;
        assert!(!result.is_valid());
    }
}
