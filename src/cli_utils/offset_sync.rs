//! Offset Sync - Consumer offset migration from Kafka to Streamline
//!
//! This module handles translating and migrating consumer group offsets
//! from Kafka to Streamline, ensuring consumers can resume from their
//! correct positions after migration.

use bytes::{BufMut, BytesMut};
use colored::Colorize;
use kafka_protocol::messages::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, ListGroupsRequest, ListGroupsResponse,
    OffsetFetchRequest, OffsetFetchResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::HashMap;
use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::time::Duration;

/// Configuration for offset sync
#[derive(Debug, Clone)]
pub struct OffsetSyncConfig {
    /// Kafka bootstrap servers
    pub kafka_bootstrap_servers: String,
    /// Streamline bootstrap servers
    pub streamline_bootstrap_servers: String,
    /// Consumer groups to sync (empty = all)
    pub groups: Vec<String>,
    /// Topics to include (empty = all)
    pub topics: Vec<String>,
    /// Dry run mode
    pub dry_run: bool,
    /// Output file for offset mapping
    pub output_file: Option<std::path::PathBuf>,
}

impl Default for OffsetSyncConfig {
    fn default() -> Self {
        Self {
            kafka_bootstrap_servers: "localhost:9092".into(),
            streamline_bootstrap_servers: "localhost:9093".into(),
            groups: vec![],
            topics: vec![],
            dry_run: true,
            output_file: None,
        }
    }
}

/// Consumer group offset information
#[derive(Debug, Clone)]
pub struct GroupOffsetInfo {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub kafka_offset: i64,
    pub streamline_offset: Option<i64>,
    pub metadata: Option<String>,
}

/// Offset sync result
#[derive(Debug)]
pub struct OffsetSyncResult {
    pub groups_synced: usize,
    pub offsets_synced: usize,
    pub errors: Vec<String>,
    pub offset_map: Vec<GroupOffsetInfo>,
}

/// Kafka client for offset operations
struct OffsetKafkaClient {
    stream: TcpStream,
    correlation_id: i32,
    client_id: String,
}

impl OffsetKafkaClient {
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
            client_id: "streamline-offset-sync".to_string(),
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

    fn list_groups(&mut self) -> crate::Result<ListGroupsResponse> {
        let request = ListGroupsRequest::default();
        self.send_request(ApiKey::ListGroups, 4, &request)
    }

    #[allow(dead_code)]
    fn describe_groups(&mut self, group_ids: &[String]) -> crate::Result<DescribeGroupsResponse> {
        use kafka_protocol::messages::GroupId;

        let groups: Vec<GroupId> = group_ids
            .iter()
            .map(|id| GroupId(StrBytes::from_string(id.clone())))
            .collect();

        let request = DescribeGroupsRequest::default().with_groups(groups);

        self.send_request(ApiKey::DescribeGroups, 5, &request)
    }

    fn fetch_offsets(
        &mut self,
        group_id: &str,
        topics: Option<&[String]>,
    ) -> crate::Result<OffsetFetchResponse> {
        use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
        use kafka_protocol::messages::{GroupId, TopicName};

        let request = if let Some(topic_list) = topics {
            let topics: Vec<OffsetFetchRequestTopic> = topic_list
                .iter()
                .map(|name| {
                    OffsetFetchRequestTopic::default()
                        .with_name(TopicName(StrBytes::from_string(name.clone())))
                        .with_partition_indexes(vec![]) // Empty = all partitions
                })
                .collect();
            OffsetFetchRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
                .with_topics(Some(topics))
        } else {
            OffsetFetchRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
                .with_topics(None)
        };

        self.send_request(ApiKey::OffsetFetch, 8, &request)
    }
}

/// Run the offset sync process
pub fn run_offset_sync(config: &OffsetSyncConfig) -> crate::Result<OffsetSyncResult> {
    print_banner();

    let mut result = OffsetSyncResult {
        groups_synced: 0,
        offsets_synced: 0,
        errors: vec![],
        offset_map: vec![],
    };

    // Step 1: Connect to Kafka
    println!();
    print_step(1, "Connecting to Kafka...");

    let kafka_addr = config
        .kafka_bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("localhost:9092");

    let mut kafka_client = match OffsetKafkaClient::connect(kafka_addr) {
        Ok(client) => {
            println!(
                "  {} Connected to Kafka at {}",
                "✓".green(),
                kafka_addr.cyan()
            );
            client
        }
        Err(e) => {
            println!("  {} Failed to connect to Kafka: {}", "✗".red(), e);
            result
                .errors
                .push(format!("Failed to connect to Kafka: {}", e));
            return Ok(result);
        }
    };

    // Step 2: Discover consumer groups
    println!();
    print_step(2, "Discovering consumer groups...");

    let groups_to_sync = if config.groups.is_empty() {
        match kafka_client.list_groups() {
            Ok(response) => {
                let groups: Vec<String> = response
                    .groups
                    .iter()
                    .filter_map(|g| {
                        let id = g.group_id.as_str().to_string();
                        // Filter out internal groups
                        if id.starts_with("__") {
                            None
                        } else {
                            Some(id)
                        }
                    })
                    .collect();
                println!("  {} Found {} consumer groups", "✓".green(), groups.len());
                groups
            }
            Err(e) => {
                println!("  {} Failed to list groups: {}", "✗".red(), e);
                result.errors.push(format!("Failed to list groups: {}", e));
                return Ok(result);
            }
        }
    } else {
        config.groups.clone()
    };

    if groups_to_sync.is_empty() {
        println!("  {} No consumer groups found", "ℹ".blue());
        return Ok(result);
    }

    for group in &groups_to_sync {
        println!("  {} {}", "•".dimmed(), group.cyan());
    }

    // Step 3: Fetch offsets for each group
    println!();
    print_step(3, "Fetching consumer offsets...");

    for group_id in &groups_to_sync {
        let topics_filter = if config.topics.is_empty() {
            None
        } else {
            Some(config.topics.as_slice())
        };

        match kafka_client.fetch_offsets(group_id, topics_filter) {
            Ok(response) => {
                for topic_resp in &response.topics {
                    let topic_name = topic_resp.name.as_str().to_string();

                    for partition in &topic_resp.partitions {
                        if partition.committed_offset >= 0 {
                            result.offset_map.push(GroupOffsetInfo {
                                group_id: group_id.clone(),
                                topic: topic_name.clone(),
                                partition: partition.partition_index,
                                kafka_offset: partition.committed_offset,
                                streamline_offset: None, // Will be calculated
                                metadata: partition
                                    .metadata
                                    .as_ref()
                                    .map(|m| m.as_str().to_string()),
                            });
                        }
                    }
                }
                result.groups_synced += 1;
            }
            Err(e) => {
                println!(
                    "  {} Failed to fetch offsets for group '{}': {}",
                    "!".yellow(),
                    group_id,
                    e
                );
                result.errors.push(format!("Group '{}': {}", group_id, e));
            }
        }
    }

    println!(
        "  {} Fetched {} offset entries from {} groups",
        "✓".green(),
        result.offset_map.len(),
        result.groups_synced
    );

    // Step 4: Calculate Streamline offsets
    println!();
    print_step(4, "Calculating Streamline offsets...");

    // For now, we assume 1:1 offset mapping (Streamline offsets match Kafka offsets)
    // In a real implementation, this would need to map based on message checksums
    // or timestamp-based correlation
    for offset_info in &mut result.offset_map {
        offset_info.streamline_offset = Some(offset_info.kafka_offset);
    }

    println!(
        "  {} Calculated {} Streamline offsets",
        "✓".green(),
        result.offset_map.len()
    );

    // Step 5: Apply offsets or output mapping
    println!();
    print_step(5, "Applying offsets...");

    if config.dry_run {
        println!("  {} Dry run mode - showing offset mapping", "ℹ".blue());
        println!();

        // Group by consumer group for display
        let mut by_group: HashMap<String, Vec<&GroupOffsetInfo>> = HashMap::new();
        for info in &result.offset_map {
            by_group
                .entry(info.group_id.clone())
                .or_default()
                .push(info);
        }

        for (group_id, offsets) in &by_group {
            println!("  {}", format!("Group: {}", group_id).cyan().bold());
            for info in offsets {
                println!(
                    "    {} {}:{} → {} (Kafka) → {} (Streamline)",
                    "•".dimmed(),
                    info.topic,
                    info.partition,
                    info.kafka_offset,
                    info.streamline_offset.unwrap_or(-1)
                );
            }
            println!();
        }

        println!("  {} Run with --execute to apply these offsets", "→".cyan());
    } else {
        // Connect to Streamline and commit offsets
        let streamline_addr = config
            .streamline_bootstrap_servers
            .split(',')
            .next()
            .unwrap_or("localhost:9093");

        match OffsetKafkaClient::connect(streamline_addr) {
            Ok(mut streamline_client) => {
                // Commit offsets to Streamline using the OffsetCommit API.
                // Group offsets by group_id for batched commits.
                use std::collections::HashMap as OffsetMap;
                let mut by_group: OffsetMap<String, Vec<&GroupOffsetInfo>> = OffsetMap::new();
                for info in &result.offset_map {
                    by_group
                        .entry(info.group_id.clone())
                        .or_default()
                        .push(info);
                }

                for (group_id, offsets) in &by_group {
                    use kafka_protocol::messages::offset_commit_request::{
                        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
                    };
                    use kafka_protocol::messages::{GroupId, OffsetCommitRequest, TopicName};

                    // Group by topic within this group
                    let mut topic_map: OffsetMap<String, Vec<&GroupOffsetInfo>> = OffsetMap::new();
                    for o in offsets {
                        topic_map.entry(o.topic.clone()).or_default().push(o);
                    }

                    let topics: Vec<OffsetCommitRequestTopic> = topic_map
                        .into_iter()
                        .map(|(topic, parts)| {
                            let partitions: Vec<OffsetCommitRequestPartition> = parts
                                .iter()
                                .map(|p| {
                                    OffsetCommitRequestPartition::default()
                                        .with_partition_index(p.partition)
                                        .with_committed_offset(
                                            p.streamline_offset.unwrap_or(p.kafka_offset),
                                        )
                                })
                                .collect();
                            OffsetCommitRequestTopic::default()
                                .with_name(TopicName(StrBytes::from_string(topic)))
                                .with_partitions(partitions)
                        })
                        .collect();

                    let request = OffsetCommitRequest::default()
                        .with_group_id(GroupId(StrBytes::from_string(
                            group_id.clone(),
                        )))
                        .with_topics(topics);

                    match streamline_client.send_request::<
                        OffsetCommitRequest,
                        kafka_protocol::messages::OffsetCommitResponse,
                    >(ApiKey::OffsetCommit, 8, &request)
                    {
                        Ok(_) => {}
                        Err(e) => {
                            result
                                .errors
                                .push(format!("OffsetCommit failed for group {}: {}", group_id, e));
                        }
                    }
                }

                result.offsets_synced = result.offset_map.len();
                println!(
                    "  {} Committed {} offsets to Streamline",
                    "✓".green(),
                    result.offsets_synced
                );
            }
            Err(e) => {
                println!("  {} Failed to connect to Streamline: {}", "✗".red(), e);
                result
                    .errors
                    .push(format!("Streamline connection failed: {}", e));
            }
        }
    }

    // Output to file if requested
    if let Some(ref output_path) = config.output_file {
        let json = serde_json::to_string_pretty(
            &result
                .offset_map
                .iter()
                .map(|o| {
                    serde_json::json!({
                        "group_id": o.group_id,
                        "topic": o.topic,
                        "partition": o.partition,
                        "kafka_offset": o.kafka_offset,
                        "streamline_offset": o.streamline_offset,
                        "metadata": o.metadata
                    })
                })
                .collect::<Vec<_>>(),
        )?;

        std::fs::write(output_path, json)?;
        println!(
            "  {} Offset mapping written to {}",
            "✓".green(),
            output_path.display()
        );
    }

    // Summary
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!("  {}", "Offset Sync Summary".bold().cyan());
    println!();
    println!(
        "  Groups processed: {}",
        result.groups_synced.to_string().green()
    );
    println!(
        "  Offsets found:    {}",
        result.offset_map.len().to_string().green()
    );
    println!(
        "  Offsets synced:   {}",
        result.offsets_synced.to_string().green()
    );
    println!(
        "  Errors:           {}",
        if result.errors.is_empty() {
            "0".green()
        } else {
            result.errors.len().to_string().red()
        }
    );

    Ok(result)
}

fn print_banner() {
    println!();
    println!("  {}", "Streamline Offset Sync".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();
    println!(
        "  {}",
        "Migrate consumer group offsets from Kafka to Streamline".dimmed()
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
    fn test_offset_sync_config_default() {
        let config = OffsetSyncConfig::default();
        assert_eq!(config.kafka_bootstrap_servers, "localhost:9092");
        assert!(config.dry_run);
    }

    #[test]
    fn test_group_offset_info() {
        let info = GroupOffsetInfo {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 0,
            kafka_offset: 100,
            streamline_offset: Some(100),
            metadata: None,
        };
        assert_eq!(info.kafka_offset, 100);
    }
}
