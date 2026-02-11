//! Built-in benchmarking tools with predefined profiles
//!
//! Provides load testing capabilities with realistic workload profiles
//! and actionable performance insights.

use bytes::Bytes;
use colored::Colorize;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Benchmark profile
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BenchmarkProfile {
    /// Quick sanity check - 1000 messages, 1KB each
    Quick,
    /// Standard benchmark - 100K messages, 1KB each
    Standard,
    /// High throughput - 1M messages, small payloads
    Throughput,
    /// Low latency - 10K messages with latency measurement
    Latency,
    /// E-commerce simulation - order events
    Ecommerce,
    /// IoT simulation - sensor data
    Iot,
    /// Logging simulation - log events
    Logging,
    /// CDC (Change Data Capture) simulation - database change events
    Cdc,
    /// AI processing benchmark - messages for embedding/classification
    Ai,
    /// Edge sync benchmark - edge-to-cloud synchronization
    Edge,
    /// Lakehouse export benchmark - columnar data export
    Lakehouse,
    /// Integration pipeline benchmark - CDC → AI → Lakehouse
    Pipeline,
    /// Runtime comparison - tokio vs sharded mode benchmark
    RuntimeComparison,
}

impl BenchmarkProfile {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "quick" => Some(Self::Quick),
            "standard" => Some(Self::Standard),
            "throughput" => Some(Self::Throughput),
            "latency" => Some(Self::Latency),
            "ecommerce" | "e-commerce" => Some(Self::Ecommerce),
            "iot" => Some(Self::Iot),
            "logging" | "logs" => Some(Self::Logging),
            "cdc" | "change-data-capture" => Some(Self::Cdc),
            "ai" | "ai-processing" => Some(Self::Ai),
            "edge" | "edge-sync" => Some(Self::Edge),
            "lakehouse" | "warehouse" => Some(Self::Lakehouse),
            "pipeline" | "integration" => Some(Self::Pipeline),
            "runtime" | "runtime-comparison" | "sharded" => Some(Self::RuntimeComparison),
            _ => None,
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::Quick => "Quick sanity check (1K msgs, ~1s)",
            Self::Standard => "Standard benchmark (100K msgs, ~30s)",
            Self::Throughput => "Max throughput test (1M msgs)",
            Self::Latency => "Latency-focused test (10K msgs)",
            Self::Ecommerce => "E-commerce order simulation",
            Self::Iot => "IoT sensor data simulation",
            Self::Logging => "Application logging simulation",
            Self::Cdc => "CDC database change event simulation (50K events)",
            Self::Ai => "AI embedding/classification benchmark (25K msgs)",
            Self::Edge => "Edge-to-cloud sync simulation (100K msgs)",
            Self::Lakehouse => "Lakehouse Parquet export benchmark (500K rows)",
            Self::Pipeline => "Full CDC → AI → Lakehouse pipeline (10K events)",
            Self::RuntimeComparison => "Runtime mode comparison (tokio vs sharded, 200K msgs)",
        }
    }

    pub fn config(&self) -> BenchmarkConfig {
        match self {
            Self::Quick => BenchmarkConfig {
                num_messages: 1_000,
                message_size: 1024,
                num_producers: 1,
                topic_name: "benchmark-quick".to_string(),
                ..Default::default()
            },
            Self::Standard => BenchmarkConfig {
                num_messages: 100_000,
                message_size: 1024,
                num_producers: 4,
                topic_name: "benchmark-standard".to_string(),
                ..Default::default()
            },
            Self::Throughput => BenchmarkConfig {
                num_messages: 1_000_000,
                message_size: 256,
                num_producers: 8,
                topic_name: "benchmark-throughput".to_string(),
                ..Default::default()
            },
            Self::Latency => BenchmarkConfig {
                num_messages: 10_000,
                message_size: 512,
                num_producers: 1,
                measure_latency: true,
                topic_name: "benchmark-latency".to_string(),
                ..Default::default()
            },
            Self::Ecommerce => BenchmarkConfig {
                num_messages: 50_000,
                message_size: 0, // Variable, set by generator
                num_producers: 4,
                message_generator: Some(MessageGenerator::Ecommerce),
                topic_name: "benchmark-orders".to_string(),
                ..Default::default()
            },
            Self::Iot => BenchmarkConfig {
                num_messages: 100_000,
                message_size: 0,
                num_producers: 8,
                message_generator: Some(MessageGenerator::Iot),
                topic_name: "benchmark-sensors".to_string(),
                ..Default::default()
            },
            Self::Logging => BenchmarkConfig {
                num_messages: 200_000,
                message_size: 0,
                num_producers: 4,
                message_generator: Some(MessageGenerator::Logging),
                topic_name: "benchmark-logs".to_string(),
                ..Default::default()
            },
            Self::Cdc => BenchmarkConfig {
                num_messages: 50_000,
                message_size: 0,
                num_producers: 4,
                message_generator: Some(MessageGenerator::Cdc),
                topic_name: "benchmark-cdc".to_string(),
                measure_latency: true,
                ..Default::default()
            },
            Self::Ai => BenchmarkConfig {
                num_messages: 25_000,
                message_size: 0,
                num_producers: 2,
                message_generator: Some(MessageGenerator::Ai),
                topic_name: "benchmark-ai".to_string(),
                measure_latency: true,
                ..Default::default()
            },
            Self::Edge => BenchmarkConfig {
                num_messages: 100_000,
                message_size: 0,
                num_producers: 8,
                message_generator: Some(MessageGenerator::Edge),
                topic_name: "benchmark-edge".to_string(),
                ..Default::default()
            },
            Self::Lakehouse => BenchmarkConfig {
                num_messages: 500_000,
                message_size: 0,
                num_producers: 4,
                message_generator: Some(MessageGenerator::Lakehouse),
                topic_name: "benchmark-lakehouse".to_string(),
                ..Default::default()
            },
            Self::Pipeline => BenchmarkConfig {
                num_messages: 10_000,
                message_size: 0,
                num_producers: 2,
                message_generator: Some(MessageGenerator::Pipeline),
                topic_name: "benchmark-pipeline".to_string(),
                measure_latency: true,
                ..Default::default()
            },
            Self::RuntimeComparison => BenchmarkConfig {
                // Designed to show differences between tokio and sharded runtime modes:
                // - 200K messages: enough to show sustained performance differences
                // - 8 partitions: exercises partition routing (partition % shard_count)
                // - 8 producers: parallelism to saturate multiple shards
                // - 1KB messages: typical payload size
                // - Latency measurement: shows P50/P99/P99.99 improvements
                // - Warmup: stabilize caches before measurement
                //
                // Run twice to compare:
                //   streamline --runtime-mode tokio &
                //   streamline-cli benchmark --profile runtime
                //
                //   streamline --runtime-mode sharded &
                //   streamline-cli benchmark --profile runtime
                num_messages: 200_000,
                message_size: 1024,
                num_producers: 8,
                num_partitions: 8,
                topic_name: "benchmark-runtime".to_string(),
                measure_latency: true,
                warmup_messages: 10_000,
                ..Default::default()
            },
        }
    }
}

/// Message generator for realistic workloads
#[derive(Debug, Clone, Copy)]
pub enum MessageGenerator {
    Ecommerce,
    Iot,
    Logging,
    /// CDC change event generator - Debezium-like format
    Cdc,
    /// AI text data generator - for embedding/classification
    Ai,
    /// Edge device data generator - sensor + metadata
    Edge,
    /// Lakehouse row generator - columnar data format
    Lakehouse,
    /// Pipeline event generator - CDC → AI enriched events
    Pipeline,
}

impl MessageGenerator {
    pub fn generate(&self, seq: u64) -> Bytes {
        match self {
            Self::Ecommerce => {
                let order = format!(
                    r#"{{"order_id":"ORD-{:08}","customer_id":"CUST-{:04}","items":[{{"sku":"SKU-{:05}","qty":{},"price":{:.2}}}],"total":{:.2},"status":"pending","created_at":"2024-01-15T10:30:00Z"}}"#,
                    seq,
                    seq % 10000,
                    seq % 50000,
                    (seq % 5) + 1,
                    (seq % 100) as f64 + 9.99,
                    ((seq % 500) as f64 + 19.99) * ((seq % 5) + 1) as f64
                );
                Bytes::from(order)
            }
            Self::Iot => {
                let reading = format!(
                    r#"{{"device_id":"sensor-{:04}","type":"temperature","value":{:.2},"unit":"celsius","battery":{},"timestamp":{}}}"#,
                    seq % 1000,
                    20.0 + (seq % 200) as f64 / 10.0,
                    100 - (seq % 50) as i32,
                    chrono::Utc::now().timestamp_millis()
                );
                Bytes::from(reading)
            }
            Self::Logging => {
                let levels = ["DEBUG", "INFO", "WARN", "ERROR"];
                let services = ["api", "worker", "scheduler", "gateway"];
                let log = format!(
                    r#"{{"level":"{}","service":"{}","message":"Processing request {}","request_id":"req-{:08}","duration_ms":{},"timestamp":"{}"}}"#,
                    levels[seq as usize % 4],
                    services[seq as usize % 4],
                    seq,
                    seq,
                    seq % 1000,
                    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                );
                Bytes::from(log)
            }
            Self::Cdc => {
                // Debezium-like CDC event format
                let operations = ["c", "u", "d", "r"]; // create, update, delete, read (snapshot)
                let tables = ["users", "orders", "products", "inventory"];
                let op = operations[seq as usize % 4];
                let table = tables[seq as usize % 4];
                let event = format!(
                    r#"{{"schema":{{"type":"struct","name":"{}.Envelope"}},"payload":{{"before":null,"after":{{"id":{},"name":"item-{}","value":{}}},"source":{{"version":"1.0","connector":"postgres","name":"dbserver1","ts_ms":{},"db":"inventory","schema":"public","table":"{}"}},"op":"{}","ts_ms":{}}}}}"#,
                    table,
                    seq,
                    seq,
                    seq % 10000,
                    chrono::Utc::now().timestamp_millis(),
                    table,
                    op,
                    chrono::Utc::now().timestamp_millis()
                );
                Bytes::from(event)
            }
            Self::Ai => {
                // Text data suitable for AI embedding/classification
                let categories = ["support", "sales", "technical", "billing", "general"];
                let sentiments = ["positive", "negative", "neutral"];
                let texts = [
                    "Customer requesting refund for order",
                    "Product inquiry about availability",
                    "Technical issue with login authentication",
                    "Billing question about subscription",
                    "General feedback about service quality",
                ];
                let text = format!(
                    r#"{{"id":"msg-{:08}","text":"{}","metadata":{{"category":"{}","sentiment":"{}","priority":{},"word_count":{}}},"embedding_model":"text-embedding-3-small","timestamp":"{}"}}"#,
                    seq,
                    texts[seq as usize % 5],
                    categories[seq as usize % 5],
                    sentiments[seq as usize % 3],
                    (seq % 3) + 1,
                    texts[seq as usize % 5].split_whitespace().count(),
                    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                );
                Bytes::from(text)
            }
            Self::Edge => {
                // Edge device sync data with offline capability metadata
                let device_types = ["gateway", "sensor", "actuator", "controller"];
                let regions = ["us-east", "us-west", "eu-central", "ap-south"];
                let edge = format!(
                    r#"{{"device_id":"edge-{:06}","device_type":"{}","region":"{}","readings":[{{"metric":"temperature","value":{:.2}}},{{"metric":"humidity","value":{:.1}}}],"sync_status":"pending","offline_duration_ms":{},"queue_depth":{},"battery_pct":{},"timestamp":{}}}"#,
                    seq % 100000,
                    device_types[seq as usize % 4],
                    regions[seq as usize % 4],
                    20.0 + (seq % 200) as f64 / 10.0,
                    30.0 + (seq % 700) as f64 / 10.0,
                    (seq % 60000) * 1000,
                    seq % 100,
                    100 - (seq % 50) as i32,
                    chrono::Utc::now().timestamp_millis()
                );
                Bytes::from(edge)
            }
            Self::Lakehouse => {
                // Columnar data format suitable for Parquet export
                let countries = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR"];
                let products = ["electronics", "clothing", "food", "furniture", "sports"];
                let row = format!(
                    r#"{{"row_id":{},"event_date":"2025-12-{}","customer_id":"C{:08}","product_category":"{}","country":"{}","quantity":{},"revenue":{:.2},"discount":{:.2},"shipping_cost":{:.2},"is_returned":{},"processing_time_ms":{}}}"#,
                    seq,
                    (seq % 28) + 1,
                    seq % 1000000,
                    products[seq as usize % 5],
                    countries[seq as usize % 8],
                    (seq % 10) + 1,
                    (seq % 1000) as f64 + 9.99,
                    (seq % 30) as f64,
                    (seq % 50) as f64 / 10.0,
                    seq % 20 == 0,
                    seq % 500
                );
                Bytes::from(row)
            }
            Self::Pipeline => {
                // Full pipeline event: CDC → AI enriched → Lakehouse ready
                let operations = ["INSERT", "UPDATE", "DELETE"];
                let tables = ["customers", "orders", "payments"];
                let ai_labels = ["high_value", "standard", "low_priority"];
                let anomaly_scores = [0.1, 0.3, 0.5, 0.7, 0.9];
                let pipeline = format!(
                    r#"{{"pipeline_id":"pipe-{:06}","stages":{{"cdc":{{"operation":"{}","table":"{}","row_id":{}}},"ai":{{"classification":"{}","confidence":{:.2},"anomaly_score":{:.2},"embedding_generated":true}},"lakehouse":{{"partition_key":"2025-12-21","format":"parquet","compression":"zstd"}}}},"latency_ms":{{"cdc_ingest":{},"ai_processing":{},"lakehouse_write":{},"total":{}}},"status":"completed","timestamp":"{}"}}"#,
                    seq,
                    operations[seq as usize % 3],
                    tables[seq as usize % 3],
                    seq,
                    ai_labels[seq as usize % 3],
                    0.7 + (seq % 30) as f64 / 100.0,
                    anomaly_scores[seq as usize % 5],
                    seq % 10,
                    10 + seq % 50,
                    5 + seq % 20,
                    15 + seq % 80,
                    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                );
                Bytes::from(pipeline)
            }
        }
    }
}

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of messages to produce
    pub num_messages: u64,
    /// Size of each message in bytes (0 for variable/generated)
    pub message_size: usize,
    /// Number of producer threads
    pub num_producers: usize,
    /// Topic name
    pub topic_name: String,
    /// Number of partitions for the topic
    pub num_partitions: i32,
    /// Data directory
    pub data_dir: PathBuf,
    /// Whether to measure per-message latency
    pub measure_latency: bool,
    /// Custom message generator
    pub message_generator: Option<MessageGenerator>,
    /// Warmup messages before measurement
    pub warmup_messages: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            num_messages: 100_000,
            message_size: 1024,
            num_producers: 4,
            topic_name: "benchmark".to_string(),
            num_partitions: 4,
            data_dir: PathBuf::from("./data"),
            measure_latency: false,
            message_generator: None,
            warmup_messages: 1000,
        }
    }
}

/// Benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    /// Total messages produced
    pub messages_produced: u64,
    /// Total bytes produced
    pub bytes_produced: u64,
    /// Total duration
    pub duration: Duration,
    /// Messages per second
    pub throughput_msgs: f64,
    /// Bytes per second
    pub throughput_bytes: f64,
    /// Latency percentiles (if measured)
    pub latency_p50: Option<Duration>,
    pub latency_p99: Option<Duration>,
    pub latency_p999: Option<Duration>,
    /// Any errors encountered
    pub errors: u64,
}

impl BenchmarkResults {
    pub fn print(&self) {
        println!();
        println!("{}", "Benchmark Results".bold().cyan());
        println!("{}", "═".repeat(50).dimmed());
        println!();

        println!(
            "  {} {}",
            "Messages:".bold(),
            format_number(self.messages_produced).green()
        );
        println!(
            "  {} {}",
            "Data:".bold(),
            format_bytes(self.bytes_produced).green()
        );
        println!(
            "  {} {}",
            "Duration:".bold(),
            format!("{:.2}s", self.duration.as_secs_f64()).cyan()
        );

        println!();
        println!("{}", "Throughput".bold());
        println!(
            "  {} {} msg/s",
            "Messages:".dimmed(),
            format_number(self.throughput_msgs as u64).green().bold()
        );
        println!(
            "  {} {}/s",
            "Bandwidth:".dimmed(),
            format_bytes(self.throughput_bytes as u64).green()
        );

        if let (Some(p50), Some(p99), Some(p999)) =
            (self.latency_p50, self.latency_p99, self.latency_p999)
        {
            println!();
            println!("{}", "Latency".bold());
            println!(
                "  {} {}",
                "P50:".dimmed(),
                format!("{:.2}ms", p50.as_secs_f64() * 1000.0).green()
            );
            println!(
                "  {} {}",
                "P99:".dimmed(),
                format!("{:.2}ms", p99.as_secs_f64() * 1000.0).yellow()
            );
            println!(
                "  {} {}",
                "P999:".dimmed(),
                format!("{:.2}ms", p999.as_secs_f64() * 1000.0).yellow()
            );
        }

        if self.errors > 0 {
            println!();
            println!(
                "  {} {} errors encountered",
                "⚠".yellow(),
                self.errors.to_string().red()
            );
        }
    }

    pub fn print_insights(&self, config: &BenchmarkConfig) {
        println!();
        println!("{}", "Performance Insights".bold().cyan());
        println!("{}", "─".repeat(50).dimmed());

        let msg_per_sec = self.throughput_msgs;
        let bytes_per_sec = self.throughput_bytes;

        // Throughput analysis
        if msg_per_sec > 100_000.0 {
            println!(
                "  {} Excellent throughput! {}+ messages/second",
                "✓".green(),
                "100K".green()
            );
        } else if msg_per_sec > 50_000.0 {
            println!(
                "  {} Good throughput: {}+ messages/second",
                "✓".green(),
                "50K".cyan()
            );
        } else if msg_per_sec > 10_000.0 {
            println!("  {} Moderate throughput: 10K+ messages/second", "ℹ".blue());
        } else {
            println!(
                "  {} Low throughput. Consider these optimizations:",
                "⚠".yellow()
            );
            println!("    {} Increase batch size", "→".cyan());
            println!("    {} Use more producer threads", "→".cyan());
            println!("    {} Check disk I/O performance", "→".cyan());
        }

        // Suggestions based on config
        if config.num_producers == 1 && msg_per_sec < 50_000.0 {
            println!();
            println!("  💡 Tip: Try multiple producers for higher throughput");
            println!(
                "    {}",
                "streamline-cli benchmark --profile throughput".cyan()
            );
        }

        // WAL recommendation
        if bytes_per_sec > 50_000_000.0 {
            println!();
            println!("  💡 Tip: For sustained high throughput, consider WAL batching:");
            println!("    Add to streamline.toml:");
            println!("    {}", "[storage.wal]".dimmed());
            println!("    {}", "batch_size = 100".dimmed());
        }

        // Latency analysis
        if let Some(p99) = self.latency_p99 {
            if p99.as_millis() < 5 {
                println!();
                println!("  {} Excellent latency! P99 under 5ms", "✓".green());
            } else if p99.as_millis() > 50 {
                println!();
                println!(
                    "  {} High P99 latency ({}ms). Consider:",
                    "⚠".yellow(),
                    p99.as_millis()
                );
                println!("    {} Using SSD storage", "→".cyan());
                println!("    {} Reducing message size", "→".cyan());
            }
        }
    }
}

/// Run a benchmark
pub fn run_benchmark(config: &BenchmarkConfig) -> crate::Result<BenchmarkResults> {
    use crate::TopicManager;

    print_benchmark_header(config);

    // Setup
    std::fs::create_dir_all(&config.data_dir)?;
    let manager = Arc::new(TopicManager::new(&config.data_dir)?);

    // Create topic
    let _ = manager.get_or_create_topic(&config.topic_name, config.num_partitions);

    // Prepare message payload
    let base_payload = if config.message_size > 0 {
        vec![b'x'; config.message_size]
    } else {
        vec![]
    };

    // Counters
    let messages_produced = Arc::new(AtomicU64::new(0));
    let bytes_produced = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let latencies: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));

    // Warmup
    if config.warmup_messages > 0 {
        print!("  Warming up ({} messages)...", config.warmup_messages);
        std::io::Write::flush(&mut std::io::stdout()).ok();

        for i in 0..config.warmup_messages {
            let payload = if let Some(gen) = &config.message_generator {
                gen.generate(i)
            } else {
                Bytes::from(base_payload.clone())
            };
            let _ = manager.append(
                &config.topic_name,
                (i % config.num_partitions as u64) as i32,
                None,
                payload,
            );
        }
        println!(" done");
    }

    // Progress display
    let progress_produced = messages_produced.clone();
    let progress_running = running.clone();
    let target = config.num_messages;

    let progress_handle = std::thread::spawn(move || {
        let start = Instant::now();
        while progress_running.load(Ordering::Relaxed) {
            let count = progress_produced.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                count as f64 / elapsed
            } else {
                0.0
            };
            let percent = (count as f64 / target as f64 * 100.0).min(100.0);

            print!(
                "\r  Progress: {:>6.1}% | {:>10} msgs | {:>10.0} msg/s   ",
                percent,
                format_number(count),
                rate
            );
            std::io::Write::flush(&mut std::io::stdout()).ok();

            if count >= target {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        println!();
    });

    // Run benchmark
    let start = Instant::now();
    let messages_per_producer = config.num_messages / config.num_producers as u64;

    let handles: Vec<_> = (0..config.num_producers)
        .map(|producer_id| {
            let manager = manager.clone();
            let topic = config.topic_name.clone();
            let num_partitions = config.num_partitions;
            let messages_produced = messages_produced.clone();
            let bytes_produced = bytes_produced.clone();
            let errors = errors.clone();
            let latencies = latencies.clone();
            let measure_latency = config.measure_latency;
            let generator = config.message_generator;
            let base_payload = base_payload.clone();
            let start_seq = producer_id as u64 * messages_per_producer;

            std::thread::spawn(move || {
                for i in 0..messages_per_producer {
                    let seq = start_seq + i;
                    let payload = if let Some(gen) = &generator {
                        gen.generate(seq)
                    } else {
                        Bytes::from(base_payload.clone())
                    };

                    let partition = (seq % num_partitions as u64) as i32;
                    let payload_len = payload.len();

                    let msg_start = if measure_latency {
                        Some(Instant::now())
                    } else {
                        None
                    };

                    match manager.append(&topic, partition, None, payload) {
                        Ok(_) => {
                            messages_produced.fetch_add(1, Ordering::Relaxed);
                            bytes_produced.fetch_add(payload_len as u64, Ordering::Relaxed);

                            if let Some(start) = msg_start {
                                latencies.lock().push(start.elapsed());
                            }
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    // Wait for producers
    for handle in handles {
        handle.join().ok();
    }

    let duration = start.elapsed();
    running.store(false, Ordering::Relaxed);
    progress_handle.join().ok();

    // Calculate results
    let total_messages = messages_produced.load(Ordering::Relaxed);
    let total_bytes = bytes_produced.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    let throughput_msgs = total_messages as f64 / duration.as_secs_f64();
    let throughput_bytes = total_bytes as f64 / duration.as_secs_f64();

    // Calculate latency percentiles
    let (p50, p99, p999) = if config.measure_latency {
        let mut lats = latencies.lock();
        lats.sort();
        let len = lats.len();
        if len > 0 {
            (
                Some(lats[len * 50 / 100]),
                Some(lats[len * 99 / 100]),
                Some(lats[(len * 999 / 1000).min(len - 1)]),
            )
        } else {
            (None, None, None)
        }
    } else {
        (None, None, None)
    };

    Ok(BenchmarkResults {
        messages_produced: total_messages,
        bytes_produced: total_bytes,
        duration,
        throughput_msgs,
        throughput_bytes,
        latency_p50: p50,
        latency_p99: p99,
        latency_p999: p999,
        errors: total_errors,
    })
}

fn print_benchmark_header(config: &BenchmarkConfig) {
    println!();
    println!("{}", "Streamline Benchmark".bold().cyan());
    println!("{}", "═".repeat(50).dimmed());
    println!();
    println!(
        "  {} {} messages",
        "Target:".bold(),
        format_number(config.num_messages).cyan()
    );
    println!(
        "  {} {} bytes",
        "Message size:".bold(),
        if config.message_size > 0 {
            format_bytes(config.message_size as u64)
        } else {
            "variable".to_string()
        }
    );
    println!(
        "  {} {}",
        "Producers:".bold(),
        config.num_producers.to_string().cyan()
    );
    println!(
        "  {} {} ({}p)",
        "Topic:".bold(),
        config.topic_name.cyan(),
        config.num_partitions
    );
    println!();
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// List available benchmark profiles
pub fn list_profiles() {
    println!();
    println!("{}", "Available Benchmark Profiles".bold().cyan());
    println!("{}", "═".repeat(50).dimmed());
    println!();

    let profiles = [
        BenchmarkProfile::Quick,
        BenchmarkProfile::Standard,
        BenchmarkProfile::Throughput,
        BenchmarkProfile::Latency,
        BenchmarkProfile::Ecommerce,
        BenchmarkProfile::Iot,
        BenchmarkProfile::Logging,
    ];

    for profile in &profiles {
        let config = profile.config();
        println!(
            "  {} {}",
            format!("{:12}", format!("{:?}", profile).to_lowercase()).cyan(),
            profile.description().dimmed()
        );
        println!(
            "    {} messages, {} producers",
            format_number(config.num_messages),
            config.num_producers
        );
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_profile_from_str() {
        assert_eq!(
            BenchmarkProfile::parse("quick"),
            Some(BenchmarkProfile::Quick)
        );
        assert_eq!(
            BenchmarkProfile::parse("ecommerce"),
            Some(BenchmarkProfile::Ecommerce)
        );
        assert_eq!(BenchmarkProfile::parse("invalid"), None);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5K");
        assert_eq!(format_number(1_500_000), "1.50M");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1_572_864), "1.50 MB");
    }

    #[test]
    fn test_message_generator() {
        let gen = MessageGenerator::Ecommerce;
        let msg = gen.generate(1);
        assert!(!msg.is_empty());
        assert!(String::from_utf8_lossy(&msg).contains("order_id"));
    }
}
