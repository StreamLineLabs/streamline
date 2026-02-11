//! Explain mode for CLI commands
//!
//! Provides detailed explanations of what commands do, helping users
//! learn how Streamline works while they use it.

use colored::Colorize;

/// Context for generating explanations
#[derive(Debug, Clone)]
pub struct ExplainContext {
    /// Whether explain mode is enabled
    enabled: bool,
    /// Steps that have been recorded
    steps: Vec<ExplainStep>,
}

/// A single step in the explanation
#[derive(Debug, Clone)]
pub struct ExplainStep {
    /// Step number (1-indexed)
    pub number: usize,
    /// Description of what happened
    pub description: String,
    /// Technical details (optional)
    pub details: Option<String>,
}

impl ExplainContext {
    /// Create a new explain context
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            steps: Vec::new(),
        }
    }

    /// Check if explain mode is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Add a step to the explanation
    pub fn step(&mut self, description: impl Into<String>) {
        if !self.enabled {
            return;
        }
        let number = self.steps.len() + 1;
        self.steps.push(ExplainStep {
            number,
            description: description.into(),
            details: None,
        });
    }

    /// Add a step with technical details
    pub fn step_with_details(
        &mut self,
        description: impl Into<String>,
        details: impl Into<String>,
    ) {
        if !self.enabled {
            return;
        }
        let number = self.steps.len() + 1;
        self.steps.push(ExplainStep {
            number,
            description: description.into(),
            details: Some(details.into()),
        });
    }

    /// Print the explanation to stdout
    pub fn print(&self) {
        if !self.enabled || self.steps.is_empty() {
            return;
        }

        println!();
        println!("{}", "What just happened:".bold().cyan());
        println!("{}", "─".repeat(50).dimmed());

        for step in &self.steps {
            println!("  {}. {}", step.number.to_string().cyan(), step.description);
            if let Some(details) = &step.details {
                println!("     {}", details.dimmed());
            }
        }
    }

    /// Print Kafka equivalent command if applicable
    pub fn print_kafka_equivalent(&self, kafka_cmd: &str) {
        if !self.enabled {
            return;
        }
        println!();
        println!("  {}: {}", "Kafka equivalent".dimmed(), kafka_cmd.yellow());
    }

    /// Clear recorded steps
    pub fn clear(&mut self) {
        self.steps.clear();
    }
}

/// Explanations for common operations
pub mod explanations {
    use super::*;

    /// Explain a produce operation
    pub fn explain_produce(
        ctx: &mut ExplainContext,
        topic: &str,
        partition: i32,
        offset: i64,
        key: Option<&str>,
        _message_size: usize,
    ) {
        ctx.step_with_details(
            "Connected to local storage",
            "Direct storage access (no network round-trip)",
        );

        if let Some(k) = key {
            ctx.step_with_details(
                format!("Computed partition from key '{}'", k),
                format!("hash(key) mod num_partitions = partition {}", partition),
            );
        } else {
            ctx.step_with_details(
                format!("Selected partition {}", partition),
                "Round-robin selection (no key provided)",
            );
        }

        ctx.step_with_details(
            format!(
                "Appended record to topic '{}' partition {}",
                topic, partition
            ),
            "Record written to active segment file",
        );

        ctx.step_with_details(
            format!("Assigned offset {}", offset),
            "Monotonically increasing sequence number",
        );

        ctx.step("Acknowledged write (acks=all for durability)");

        ctx.print();
        ctx.print_kafka_equivalent(&format!(
            "kafka-console-producer.sh --broker-list localhost:9092 --topic {}",
            topic
        ));
    }

    /// Explain a consume operation
    pub fn explain_consume(
        ctx: &mut ExplainContext,
        topic: &str,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
        record_count: usize,
    ) {
        ctx.step_with_details(
            "Connected to local storage",
            "Direct storage access (zero-copy reads when possible)",
        );

        ctx.step_with_details(
            format!(
                "Located segment files for topic '{}' partition {}",
                topic, partition
            ),
            format!(
                "Segment files in data/topics/{}/partition-{}/",
                topic, partition
            ),
        );

        ctx.step_with_details(
            format!("Seeked to offset {}", start_offset),
            "Used sparse index for O(log n) lookup",
        );

        ctx.step_with_details(
            format!(
                "Read {} record(s) from offset {} to {}",
                record_count, start_offset, end_offset
            ),
            "Sequential read for optimal disk I/O",
        );

        ctx.print();
        ctx.print_kafka_equivalent(&format!(
            "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {} --from-beginning",
            topic
        ));
    }

    /// Explain topic creation
    pub fn explain_create_topic(
        ctx: &mut ExplainContext,
        topic: &str,
        partitions: i32,
        retention_ms: i64,
    ) {
        ctx.step_with_details(
            format!("Validated topic name '{}'", topic),
            "Checked: not empty, valid characters, not reserved",
        );

        ctx.step_with_details(
            format!("Created {} partition(s)", partitions),
            "Each partition is an independent ordered log",
        );

        ctx.step_with_details(
            "Created directory structure",
            format!("data/topics/{}/partition-0..{}/", topic, partitions - 1),
        );

        ctx.step_with_details(
            "Initialized metadata",
            format!(
                "retention_ms={}, segment_bytes=104857600",
                if retention_ms < 0 {
                    "infinite".to_string()
                } else {
                    format!("{}", retention_ms)
                }
            ),
        );

        ctx.step("Topic ready for produce/consume");

        ctx.print();
        ctx.print_kafka_equivalent(&format!(
            "kafka-topics.sh --create --topic {} --partitions {} --bootstrap-server localhost:9092",
            topic, partitions
        ));
    }

    /// Explain consumer group join
    pub fn explain_group_join(
        ctx: &mut ExplainContext,
        group_id: &str,
        member_id: &str,
        topics: &[String],
        assigned_partitions: &[(String, i32)],
    ) {
        ctx.step_with_details(
            format!("Joined consumer group '{}'", group_id),
            format!("Member ID: {}", member_id),
        );

        ctx.step_with_details(
            format!("Subscribed to {} topic(s)", topics.len()),
            format!("Topics: {}", topics.join(", ")),
        );

        ctx.step_with_details(
            "Performed partition assignment",
            "Using range assignor strategy",
        );

        ctx.step_with_details(
            format!("Assigned {} partition(s)", assigned_partitions.len()),
            assigned_partitions
                .iter()
                .map(|(t, p)| format!("{}[{}]", t, p))
                .collect::<Vec<_>>()
                .join(", "),
        );

        ctx.step("Ready to consume from assigned partitions");

        ctx.print();
    }

    /// Explain offset commit
    pub fn explain_offset_commit(
        ctx: &mut ExplainContext,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) {
        ctx.step_with_details(
            format!("Committed offset {} for {}[{}]", offset, topic, partition),
            format!("Consumer group: {}", group_id),
        );

        ctx.step_with_details(
            "Stored offset in consumer group state",
            "Persisted to data/consumer-groups/",
        );

        ctx.step_with_details(
            "Next consume will start from this offset",
            "Ensures exactly-once semantics for this consumer",
        );

        ctx.print();
    }

    /// Explain a fetch request
    pub fn explain_fetch(
        ctx: &mut ExplainContext,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        max_bytes: i32,
        records_fetched: usize,
        bytes_fetched: usize,
    ) {
        ctx.step_with_details(
            format!("Fetch request for {}[{}]", topic, partition),
            format!(
                "Starting offset: {}, max bytes: {}",
                fetch_offset, max_bytes
            ),
        );

        ctx.step_with_details(
            "Located data in segment files",
            "Used index for fast offset lookup",
        );

        ctx.step_with_details(
            format!(
                "Fetched {} records ({} bytes)",
                records_fetched, bytes_fetched
            ),
            "Batched for network efficiency",
        );

        ctx.print();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explain_context_disabled() {
        let mut ctx = ExplainContext::new(false);
        ctx.step("This should not be recorded");
        assert!(ctx.steps.is_empty());
    }

    #[test]
    fn test_explain_context_enabled() {
        let mut ctx = ExplainContext::new(true);
        ctx.step("Step 1");
        ctx.step_with_details("Step 2", "Details");
        assert_eq!(ctx.steps.len(), 2);
        assert_eq!(ctx.steps[0].number, 1);
        assert_eq!(ctx.steps[1].number, 2);
        assert!(ctx.steps[1].details.is_some());
    }

    #[test]
    fn test_explain_context_clear() {
        let mut ctx = ExplainContext::new(true);
        ctx.step("Step 1");
        ctx.clear();
        assert!(ctx.steps.is_empty());
    }
}
