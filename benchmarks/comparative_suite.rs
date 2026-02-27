//! Comparative benchmark test definitions for Streamline vs competitors.
//!
//! These define the test matrix used by CI to generate benchmark reports.
//! This module contains reference data and test case definitions — it is not
//! an executable benchmark runner.

/// A single benchmark test case definition.
pub struct BenchmarkTestCase {
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub message_count: u64,
    pub message_size_bytes: usize,
    pub producer_count: usize,
    pub consumer_count: usize,
    pub partitions: usize,
}

/// The full set of comparative benchmark tests.
pub const COMPARATIVE_TESTS: &[BenchmarkTestCase] = &[
    // Startup time
    BenchmarkTestCase {
        name: "cold_start",
        category: "startup",
        description: "Time from binary launch to first accepted connection",
        message_count: 0,
        message_size_bytes: 0,
        producer_count: 0,
        consumer_count: 0,
        partitions: 1,
    },
    // Throughput
    BenchmarkTestCase {
        name: "throughput_1kb",
        category: "throughput",
        description: "Sustained throughput with 1KB messages",
        message_count: 1_000_000,
        message_size_bytes: 1024,
        producer_count: 4,
        consumer_count: 4,
        partitions: 8,
    },
    BenchmarkTestCase {
        name: "throughput_100b",
        category: "throughput",
        description: "Small message throughput",
        message_count: 5_000_000,
        message_size_bytes: 100,
        producer_count: 4,
        consumer_count: 4,
        partitions: 8,
    },
    // Latency
    BenchmarkTestCase {
        name: "p99_latency",
        category: "latency",
        description: "P99 end-to-end latency",
        message_count: 100_000,
        message_size_bytes: 256,
        producer_count: 1,
        consumer_count: 1,
        partitions: 1,
    },
    BenchmarkTestCase {
        name: "p99_latency_loaded",
        category: "latency",
        description: "P99 latency under load",
        message_count: 500_000,
        message_size_bytes: 512,
        producer_count: 8,
        consumer_count: 8,
        partitions: 16,
    },
    // Resource usage
    BenchmarkTestCase {
        name: "memory_idle",
        category: "resource",
        description: "Memory usage with no activity",
        message_count: 0,
        message_size_bytes: 0,
        producer_count: 0,
        consumer_count: 0,
        partitions: 0,
    },
    BenchmarkTestCase {
        name: "memory_100_topics",
        category: "resource",
        description: "Memory with 100 topics",
        message_count: 0,
        message_size_bytes: 0,
        producer_count: 0,
        consumer_count: 0,
        partitions: 300,
    },
    BenchmarkTestCase {
        name: "binary_size",
        category: "resource",
        description: "Binary size on disk",
        message_count: 0,
        message_size_bytes: 0,
        producer_count: 0,
        consumer_count: 0,
        partitions: 0,
    },
    // End-to-end
    BenchmarkTestCase {
        name: "consumer_group_rebalance",
        category: "e2e",
        description: "Time for consumer group rebalance",
        message_count: 100_000,
        message_size_bytes: 256,
        producer_count: 1,
        consumer_count: 4,
        partitions: 4,
    },
    BenchmarkTestCase {
        name: "topic_creation_100",
        category: "e2e",
        description: "Time to create 100 topics",
        message_count: 0,
        message_size_bytes: 0,
        producer_count: 0,
        consumer_count: 0,
        partitions: 300,
    },
];

/// Platforms included in comparative benchmarks.
pub const PLATFORMS: &[&str] = &[
    "Streamline",
    "Apache Kafka",
    "Redpanda",
    "NATS JetStream",
    "Apache Pulsar",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparative_tests_count() {
        assert_eq!(COMPARATIVE_TESTS.len(), 10);
    }

    #[test]
    fn test_platforms_count() {
        assert_eq!(PLATFORMS.len(), 5);
        assert!(PLATFORMS.contains(&"Streamline"));
    }

    #[test]
    fn test_all_tests_have_names() {
        for test in COMPARATIVE_TESTS {
            assert!(!test.name.is_empty());
            assert!(!test.category.is_empty());
            assert!(!test.description.is_empty());
        }
    }

    #[test]
    fn test_categories_are_valid() {
        let valid = ["startup", "throughput", "latency", "resource", "e2e"];
        for test in COMPARATIVE_TESTS {
            assert!(
                valid.contains(&test.category),
                "Invalid category '{}' for test '{}'",
                test.category,
                test.name
            );
        }
    }

    #[test]
    fn test_cold_start_definition() {
        let cold_start = &COMPARATIVE_TESTS[0];
        assert_eq!(cold_start.name, "cold_start");
        assert_eq!(cold_start.category, "startup");
        assert_eq!(cold_start.message_count, 0);
        assert_eq!(cold_start.partitions, 1);
    }

    #[test]
    fn test_throughput_1kb_definition() {
        let t = COMPARATIVE_TESTS
            .iter()
            .find(|t| t.name == "throughput_1kb")
            .unwrap();
        assert_eq!(t.message_count, 1_000_000);
        assert_eq!(t.message_size_bytes, 1024);
        assert_eq!(t.producer_count, 4);
        assert_eq!(t.consumer_count, 4);
        assert_eq!(t.partitions, 8);
    }
}
