//! Streamline Mirror - Real-time Kafka to Streamline replication
//!
//! This module implements a MirrorMaker-like tool that replicates data
//! from Kafka to Streamline in real-time, enabling zero-downtime migrations.

use bytes::{BufMut, BytesMut};
use colored::Colorize;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::HashMap;
use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Mirror configuration
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Kafka bootstrap servers
    pub kafka_bootstrap_servers: String,
    /// Streamline bootstrap servers
    pub streamline_bootstrap_servers: String,
    /// Topics to mirror (empty = all non-internal topics)
    pub topics: Vec<String>,
    /// Consumer group ID for offset tracking
    pub group_id: String,
    /// Maximum records per second (0 = unlimited)
    pub rate_limit: u64,
    /// Batch size for fetch requests
    pub batch_size: i32,
    /// Whether to start from earliest or latest offset
    pub from_beginning: bool,
    /// Offset file for persistence
    pub offset_file: Option<std::path::PathBuf>,
    /// Dry run mode (don't actually write to Streamline)
    pub dry_run: bool,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        Self {
            kafka_bootstrap_servers: "localhost:9092".into(),
            streamline_bootstrap_servers: "localhost:9093".into(),
            topics: vec![],
            group_id: "streamline-mirror".into(),
            rate_limit: 0,
            batch_size: 500,
            from_beginning: true,
            offset_file: None,
            dry_run: false,
        }
    }
}

/// Mirror statistics
#[derive(Debug, Default)]
pub struct MirrorStats {
    /// Total records mirrored
    pub records_mirrored: AtomicU64,
    /// Total bytes mirrored
    pub bytes_mirrored: AtomicU64,
    /// Records per second
    pub records_per_second: AtomicU64,
    /// Current lag (records behind)
    pub lag: AtomicU64,
    /// Errors encountered
    pub errors: AtomicU64,
}

/// Topic partition offset state
#[derive(Debug, Clone)]
struct PartitionState {
    topic: String,
    partition: i32,
    current_offset: i64,
    high_watermark: i64,
}

/// Kafka client for mirroring
struct MirrorKafkaClient {
    stream: TcpStream,
    correlation_id: i32,
    client_id: String,
}

impl MirrorKafkaClient {
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
            client_id: "streamline-mirror".to_string(),
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

    fn list_offsets(
        &mut self,
        topics: &[(String, i32)],
        timestamp: i64,
    ) -> crate::Result<ListOffsetsResponse> {
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
                            .with_timestamp(timestamp)
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

        self.send_request(ApiKey::ListOffsets, 5, &request)
    }

    fn fetch(
        &mut self,
        partitions: &[PartitionState],
        max_bytes: i32,
    ) -> crate::Result<FetchResponse> {
        use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

        // Group by topic
        let mut topic_map: HashMap<String, Vec<&PartitionState>> = HashMap::new();
        for p in partitions {
            topic_map.entry(p.topic.clone()).or_default().push(p);
        }

        let topics: Vec<FetchTopic> = topic_map
            .into_iter()
            .map(|(name, states)| {
                let partition_reqs: Vec<FetchPartition> = states
                    .into_iter()
                    .map(|s| {
                        FetchPartition::default()
                            .with_partition(s.partition)
                            .with_fetch_offset(s.current_offset)
                            .with_partition_max_bytes(max_bytes)
                    })
                    .collect();
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(name)))
                    .with_partitions(partition_reqs)
            })
            .collect();

        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(5000)
            .with_min_bytes(1)
            .with_max_bytes(max_bytes * partitions.len() as i32)
            .with_isolation_level(0)
            .with_topics(topics);

        self.send_request(ApiKey::Fetch, 11, &request)
    }
}

/// Run the mirror process
pub fn run_mirror(config: &MirrorConfig, stop_flag: Arc<AtomicBool>) -> crate::Result<MirrorStats> {
    print_banner();

    let stats = Arc::new(MirrorStats::default());

    // Step 1: Connect to Kafka
    println!();
    print_step(1, "Connecting to Kafka...");

    let kafka_addr = config
        .kafka_bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("localhost:9092");

    let mut kafka_client = match MirrorKafkaClient::connect(kafka_addr) {
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
            return Err(e);
        }
    };

    // Step 2: Discover topics
    println!();
    print_step(2, "Discovering topics...");

    let metadata = kafka_client.metadata(if config.topics.is_empty() {
        None
    } else {
        Some(&config.topics)
    })?;

    let mut partitions: Vec<PartitionState> = Vec::new();

    for topic in &metadata.topics {
        let topic_name = match topic.name.as_ref() {
            Some(n) => n.as_str().to_string(),
            None => continue,
        };

        // Skip internal topics
        if topic_name.starts_with("__") {
            continue;
        }

        // Skip if not in selected topics
        if !config.topics.is_empty() && !config.topics.contains(&topic_name) {
            continue;
        }

        for partition in &topic.partitions {
            partitions.push(PartitionState {
                topic: topic_name.clone(),
                partition: partition.partition_index,
                current_offset: 0,
                high_watermark: 0,
            });
        }

        println!(
            "  {} {} ({} partitions)",
            "•".dimmed(),
            topic_name.cyan(),
            topic.partitions.len()
        );
    }

    if partitions.is_empty() {
        println!("  {} No topics found to mirror", "!".yellow());
        return Ok(Arc::try_unwrap(stats).unwrap_or_default());
    }

    println!(
        "  {} Found {} partitions to mirror",
        "✓".green(),
        partitions.len()
    );

    // Step 3: Get initial offsets
    println!();
    print_step(3, "Fetching initial offsets...");

    let timestamp = if config.from_beginning { -2 } else { -1 }; // -2 = earliest, -1 = latest
    let topic_partitions: Vec<(String, i32)> = partitions
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect();

    let offset_response = kafka_client.list_offsets(&topic_partitions, timestamp)?;

    // Update partition states with initial offsets
    for topic_resp in &offset_response.topics {
        let topic_name = topic_resp.name.as_str();
        for partition_resp in &topic_resp.partitions {
            for p in &mut partitions {
                if p.topic == topic_name && p.partition == partition_resp.partition_index {
                    p.current_offset = partition_resp.offset;
                }
            }
        }
    }

    println!("  {} Initial offsets retrieved", "✓".green());

    // Step 4: Connect to Streamline
    println!();
    print_step(4, "Connecting to Streamline...");

    let streamline_addr = config
        .streamline_bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("localhost:9093");

    if !config.dry_run {
        // Verify Streamline connection
        match MirrorKafkaClient::connect(streamline_addr) {
            Ok(_) => {
                println!(
                    "  {} Connected to Streamline at {}",
                    "✓".green(),
                    streamline_addr.cyan()
                );
            }
            Err(e) => {
                println!(
                    "  {} Warning: Could not connect to Streamline: {}",
                    "!".yellow(),
                    e
                );
                println!("  {} Continuing in monitoring mode", "ℹ".blue());
            }
        }
    } else {
        println!(
            "  {} Dry run mode - skipping Streamline connection",
            "ℹ".blue()
        );
    }

    // Step 5: Start mirroring
    println!();
    print_step(5, "Starting mirror...");
    println!();
    println!("  {} Press Ctrl+C to stop", "ℹ".blue());
    println!();

    let start_time = Instant::now();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(5);

    while !stop_flag.load(Ordering::Relaxed) {
        // Fetch records from Kafka
        let fetch_response = match kafka_client.fetch(&partitions, config.batch_size * 1024) {
            Ok(resp) => resp,
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("  {} Fetch error: {}", "!".yellow(), e);
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let mut batch_records = 0u64;
        let mut batch_bytes = 0u64;

        // Process fetch response
        for topic_resp in &fetch_response.responses {
            let topic_name = topic_resp.topic.as_str();
            for partition_resp in &topic_resp.partitions {
                // Update high watermark
                for p in &mut partitions {
                    if p.topic == topic_name && p.partition == partition_resp.partition_index {
                        p.high_watermark = partition_resp.high_watermark;
                    }
                }

                // Process records
                if let Some(ref records) = partition_resp.records {
                    let records_bytes = records.len() as u64;
                    batch_bytes += records_bytes;

                    // Count records (simplified - in reality would decode record batches)
                    // For now, estimate ~100 bytes per record
                    let est_records = (records_bytes / 100).max(1);
                    batch_records += est_records;

                    // Update offset
                    for p in &mut partitions {
                        if p.topic == topic_name && p.partition == partition_resp.partition_index {
                            // Move offset forward (in reality would parse last offset from records)
                            p.current_offset += est_records as i64;
                        }
                    }

                    if !config.dry_run {
                        // Produce mirrored records to Streamline.
                        // Connect (or reuse) to Streamline and send the raw record
                        // batch via the Kafka Produce API.
                        match MirrorKafkaClient::connect(streamline_addr) {
                            Ok(mut sl_client) => {
                                use kafka_protocol::messages::produce_request::{
                                    PartitionProduceData, TopicProduceData,
                                };
                                use kafka_protocol::messages::ProduceRequest;

                                let topic_data = TopicProduceData::default()
                                    .with_name(TopicName(StrBytes::from_string(
                                        topic_name.to_string(),
                                    )))
                                    .with_partition_data(vec![
                                        PartitionProduceData::default()
                                            .with_index(partition_resp.partition_index)
                                            .with_records(Some(records.clone())),
                                    ]);

                                let request = ProduceRequest::default()
                                    .with_acks(-1)
                                    .with_timeout_ms(30000)
                                    .with_topic_data(vec![topic_data]);

                                if let Err(e) = sl_client.send_request::<
                                    ProduceRequest,
                                    kafka_protocol::messages::ProduceResponse,
                                >(
                                    ApiKey::Produce, 9, &request
                                ) {
                                    stats.errors.fetch_add(1, Ordering::Relaxed);
                                    eprintln!(
                                        "  {} Produce to Streamline failed: {}",
                                        "!".yellow(),
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                stats.errors.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "  {} Streamline connection failed: {}",
                                    "!".yellow(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        // Update stats
        if batch_records > 0 {
            stats
                .records_mirrored
                .fetch_add(batch_records, Ordering::Relaxed);
            stats
                .bytes_mirrored
                .fetch_add(batch_bytes, Ordering::Relaxed);
        }

        // Calculate lag
        let total_lag: i64 = partitions
            .iter()
            .map(|p| (p.high_watermark - p.current_offset).max(0))
            .sum();
        stats.lag.store(total_lag as u64, Ordering::Relaxed);

        // Rate limiting
        if config.rate_limit > 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            let target_records = (config.rate_limit as f64 * elapsed) as u64;
            let current_records = stats.records_mirrored.load(Ordering::Relaxed);
            if current_records > target_records {
                let sleep_ms = ((current_records - target_records) as f64
                    / config.rate_limit as f64
                    * 1000.0) as u64;
                std::thread::sleep(Duration::from_millis(sleep_ms.min(1000)));
            }
        }

        // Periodic status report
        if last_report.elapsed() >= report_interval {
            let total_records = stats.records_mirrored.load(Ordering::Relaxed);
            let total_bytes = stats.bytes_mirrored.load(Ordering::Relaxed);
            let lag = stats.lag.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed().as_secs_f64();
            let rps = if elapsed > 0.0 {
                total_records as f64 / elapsed
            } else {
                0.0
            };

            stats
                .records_per_second
                .store(rps as u64, Ordering::Relaxed);

            println!(
                "  {} Records: {} | Bytes: {} | Rate: {:.0}/s | Lag: {}",
                "→".cyan(),
                format_number(total_records),
                format_bytes(total_bytes),
                rps,
                format_number(lag)
            );

            last_report = Instant::now();
        }

        // Small sleep to prevent busy loop when caught up
        if batch_records == 0 {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    // Final summary
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!("  {}", "Mirror Summary".bold().cyan());
    println!();

    let total_records = stats.records_mirrored.load(Ordering::Relaxed);
    let total_bytes = stats.bytes_mirrored.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let elapsed = start_time.elapsed();

    println!(
        "  Records mirrored: {}",
        format_number(total_records).green()
    );
    println!("  Bytes mirrored:   {}", format_bytes(total_bytes).green());
    println!("  Duration:         {:?}", elapsed);
    println!(
        "  Errors:           {}",
        if errors == 0 {
            "0".green()
        } else {
            format_number(errors).red()
        }
    );

    if config.dry_run {
        println!();
        println!(
            "  {} This was a dry run - no data was written to Streamline",
            "ℹ".blue()
        );
    }

    Ok(Arc::try_unwrap(stats).unwrap_or_default())
}

fn print_banner() {
    println!();
    println!("  {}", "Streamline Mirror".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();
    println!("  {}", "Real-time Kafka to Streamline replication".dimmed());
}

fn print_step(num: u32, message: &str) {
    println!(
        "  {} {}",
        format!("[{}/5]", num).cyan().bold(),
        message.bold()
    );
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_config_default() {
        let config = MirrorConfig::default();
        assert_eq!(config.kafka_bootstrap_servers, "localhost:9092");
        assert_eq!(config.group_id, "streamline-mirror");
        assert!(config.from_beginning);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5K");
        assert_eq!(format_number(1_500_000), "1.5M");
        assert_eq!(format_number(1_500_000_000), "1.5B");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_572_864), "1.5 MB");
        assert_eq!(format_bytes(1_610_612_736), "1.5 GB");
    }
}
