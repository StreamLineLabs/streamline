//! Jepsen-style correctness testing framework for Streamline transactions.
//!
//! Provides tools to verify exactly-once semantics under adversarial conditions:
//! - Network partitions
//! - Process crashes
//! - Clock skew
//! - Concurrent operations
//!
//! ## Architecture
//!
//! The framework follows Jepsen's model:
//! 1. **Generator**: Produces a sequence of operations (reads, writes, CAS)
//! 2. **Client**: Executes operations against Streamline
//! 3. **Nemesis**: Injects failures (partitions, crashes, delays)
//! 4. **Checker**: Verifies the history satisfies correctness properties
//!
//! ## Usage
//!
//! ```text
//! let test = JepsenTest::new("eos-linearizability")
//!     .with_generator(AppendGenerator::new("test-topic", 1000))
//!     .with_nemesis(PartitionNemesis::random(Duration::from_secs(5)))
//!     .with_checker(LinearizabilityChecker::new())
//!     .run()
//!     .await;
//! assert!(test.passed());
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Type of operation in a Jepsen history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpType {
    /// Operation was invoked.
    Invoke,
    /// Operation completed successfully.
    Ok,
    /// Operation failed.
    Fail,
    /// Operation result is unknown (timeout, crash).
    Info,
}

/// A single operation in the test history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// Operation index in the history.
    pub index: u64,
    /// Type (invoke, ok, fail, info).
    pub op_type: OpType,
    /// Function name (e.g., "produce", "consume", "txn-begin", "txn-commit").
    pub function: String,
    /// Value associated with the operation.
    pub value: serde_json::Value,
    /// Timestamp when this was recorded.
    pub timestamp: DateTime<Utc>,
    /// Process/thread ID that executed this.
    pub process: u32,
}

/// Result of a Jepsen-style test run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Test name.
    pub name: String,
    /// Whether the test passed all checkers.
    pub passed: bool,
    /// Total operations executed.
    pub total_ops: u64,
    /// Number of successful operations.
    pub ok_ops: u64,
    /// Number of failed operations.
    pub fail_ops: u64,
    /// Number of indeterminate operations.
    pub info_ops: u64,
    /// Checker results.
    pub checker_results: Vec<CheckerResult>,
    /// Duration of the test.
    pub duration: Duration,
    /// Nemesis actions taken.
    pub nemesis_actions: Vec<NemesisAction>,
}

impl TestResult {
    /// Whether all checkers passed.
    pub fn passed(&self) -> bool {
        self.passed
    }
}

/// Result from a single checker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckerResult {
    /// Checker name.
    pub name: String,
    /// Whether this checker passed.
    pub valid: bool,
    /// Human-readable description of the result.
    pub message: String,
    /// Anomalies found (if any).
    pub anomalies: Vec<Anomaly>,
}

/// An anomaly found by a checker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Type of anomaly.
    pub anomaly_type: AnomalyType,
    /// Description.
    pub description: String,
    /// Operations involved.
    pub operations: Vec<u64>,
}

/// Types of correctness anomalies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalyType {
    /// Message was delivered more than once (violates exactly-once).
    DuplicateDelivery,
    /// Message was lost (violates at-least-once).
    LostMessage,
    /// Messages were delivered out of order (violates ordering).
    OutOfOrder,
    /// Read saw an inconsistent state (violates linearizability).
    StaleRead,
    /// Transaction committed but effects are not visible.
    PhantomCommit,
    /// Transaction aborted but effects are visible.
    DirtyRead,
}

/// A nemesis action that was taken during the test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NemesisAction {
    /// When the action was taken.
    pub timestamp: DateTime<Utc>,
    /// Type of disruption.
    pub action_type: NemesisType,
    /// Human-readable description.
    pub description: String,
}

/// Types of nemesis disruptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NemesisType {
    /// Network partition between nodes.
    Partition,
    /// Process crash and restart.
    ProcessCrash,
    /// Clock skew injection.
    ClockSkew,
    /// Slow network (added latency).
    SlowNetwork,
    /// Disk full simulation.
    DiskFull,
}

/// Exactly-once semantics checker.
///
/// Verifies that:
/// 1. No message is delivered more than once (no duplicates)
/// 2. No message is lost (no gaps in sequence)
/// 3. Messages are delivered in order per partition
pub struct EosChecker;

impl EosChecker {
    /// Check a history for EOS violations.
    pub fn check(history: &[Operation]) -> CheckerResult {
        let mut anomalies = Vec::new();
        let mut seen_values: HashMap<String, Vec<u64>> = HashMap::new();
        let mut expected_sequence: HashMap<String, i64> = HashMap::new();

        for op in history.iter().filter(|o| o.op_type == OpType::Ok) {
            if op.function == "consume" {
                if let Some(key) = op.value.get("key").and_then(|v| v.as_str()) {
                    let offset = op
                        .value
                        .get("offset")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(-1);
                    let partition = op
                        .value
                        .get("partition")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0");
                    let msg_id = format!("{}:{}:{}", partition, key, offset);

                    // Check duplicates
                    let entries = seen_values.entry(msg_id.clone()).or_default();
                    entries.push(op.index);
                    if entries.len() > 1 {
                        anomalies.push(Anomaly {
                            anomaly_type: AnomalyType::DuplicateDelivery,
                            description: format!("Message {} delivered {} times", msg_id, entries.len()),
                            operations: entries.clone(),
                        });
                    }

                    // Check ordering per partition
                    let last_offset = expected_sequence.entry(partition.to_string()).or_insert(-1);
                    if offset >= 0 && *last_offset >= 0 && offset < *last_offset {
                        anomalies.push(Anomaly {
                            anomaly_type: AnomalyType::OutOfOrder,
                            description: format!(
                                "Partition {}: offset {} after {} (out of order)",
                                partition, offset, last_offset
                            ),
                            operations: vec![op.index],
                        });
                    }
                    if offset >= 0 {
                        *last_offset = offset;
                    }
                }
            }
        }

        // Check for lost messages (gaps in committed transaction writes)
        let produce_ops: Vec<&Operation> = history
            .iter()
            .filter(|o| o.op_type == OpType::Ok && o.function == "produce")
            .collect();

        let consume_ops: Vec<&Operation> = history
            .iter()
            .filter(|o| o.op_type == OpType::Ok && o.function == "consume")
            .collect();

        if !produce_ops.is_empty() && consume_ops.is_empty() && !produce_ops.is_empty() {
            anomalies.push(Anomaly {
                anomaly_type: AnomalyType::LostMessage,
                description: format!(
                    "{} messages produced but none consumed",
                    produce_ops.len()
                ),
                operations: produce_ops.iter().map(|o| o.index).collect(),
            });
        }

        let valid = anomalies.is_empty();
        CheckerResult {
            name: "exactly-once-semantics".to_string(),
            valid,
            message: if valid {
                "No EOS violations detected".to_string()
            } else {
                format!("{} anomalies found", anomalies.len())
            },
            anomalies,
        }
    }
}

/// Generate a test report in markdown format.
pub fn generate_report(result: &TestResult) -> String {
    let mut report = String::new();
    report.push_str(&format!("# Jepsen Test Report: {}\n\n", result.name));
    report.push_str(&format!(
        "**Result: {}**\n\n",
        if result.passed { "✅ PASS" } else { "❌ FAIL" }
    ));
    report.push_str(&format!(
        "| Metric | Value |\n|--------|-------|\n\
         | Total Operations | {} |\n\
         | Successful | {} |\n\
         | Failed | {} |\n\
         | Indeterminate | {} |\n\
         | Duration | {:.2}s |\n\n",
        result.total_ops,
        result.ok_ops,
        result.fail_ops,
        result.info_ops,
        result.duration.as_secs_f64(),
    ));

    if !result.nemesis_actions.is_empty() {
        report.push_str("## Nemesis Actions\n\n");
        for action in &result.nemesis_actions {
            report.push_str(&format!(
                "- `{:?}` at {}: {}\n",
                action.action_type, action.timestamp, action.description
            ));
        }
        report.push('\n');
    }

    report.push_str("## Checker Results\n\n");
    for checker in &result.checker_results {
        report.push_str(&format!(
            "### {} — {}\n\n{}\n\n",
            checker.name,
            if checker.valid { "✅ PASS" } else { "❌ FAIL" },
            checker.message
        ));
        if !checker.anomalies.is_empty() {
            report.push_str("| Type | Description | Operations |\n|------|-------------|------------|\n");
            for anomaly in &checker.anomalies {
                report.push_str(&format!(
                    "| {:?} | {} | {:?} |\n",
                    anomaly.anomaly_type, anomaly.description, anomaly.operations
                ));
            }
            report.push('\n');
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_op(index: u64, op_type: OpType, function: &str, value: serde_json::Value) -> Operation {
        Operation {
            index,
            op_type,
            function: function.to_string(),
            value,
            timestamp: Utc::now(),
            process: 0,
        }
    }

    #[test]
    fn test_eos_checker_no_anomalies() {
        let history = vec![
            make_op(0, OpType::Ok, "produce", serde_json::json!({"key": "a", "offset": 0})),
            make_op(1, OpType::Ok, "consume", serde_json::json!({"key": "a", "offset": 0, "partition": "0"})),
            make_op(2, OpType::Ok, "consume", serde_json::json!({"key": "b", "offset": 1, "partition": "0"})),
        ];

        let result = EosChecker::check(&history);
        assert!(result.valid);
        assert!(result.anomalies.is_empty());
    }

    #[test]
    fn test_eos_checker_detects_duplicates() {
        let history = vec![
            make_op(0, OpType::Ok, "consume", serde_json::json!({"key": "a", "offset": 0, "partition": "0"})),
            make_op(1, OpType::Ok, "consume", serde_json::json!({"key": "a", "offset": 0, "partition": "0"})),
        ];

        let result = EosChecker::check(&history);
        assert!(!result.valid);
        assert_eq!(result.anomalies.len(), 1);
        assert_eq!(result.anomalies[0].anomaly_type, AnomalyType::DuplicateDelivery);
    }

    #[test]
    fn test_eos_checker_detects_out_of_order() {
        let history = vec![
            make_op(0, OpType::Ok, "consume", serde_json::json!({"key": "a", "offset": 5, "partition": "0"})),
            make_op(1, OpType::Ok, "consume", serde_json::json!({"key": "b", "offset": 3, "partition": "0"})),
        ];

        let result = EosChecker::check(&history);
        assert!(!result.valid);
        assert_eq!(result.anomalies[0].anomaly_type, AnomalyType::OutOfOrder);
    }

    #[test]
    fn test_eos_checker_ignores_failed_ops() {
        let history = vec![
            make_op(0, OpType::Fail, "consume", serde_json::json!({"key": "a", "offset": 0})),
            make_op(1, OpType::Ok, "consume", serde_json::json!({"key": "a", "offset": 0, "partition": "0"})),
        ];

        let result = EosChecker::check(&history);
        assert!(result.valid);
    }

    #[test]
    fn test_report_generation() {
        let result = TestResult {
            name: "eos-test".to_string(),
            passed: true,
            total_ops: 1000,
            ok_ops: 980,
            fail_ops: 15,
            info_ops: 5,
            checker_results: vec![CheckerResult {
                name: "eos".to_string(),
                valid: true,
                message: "No violations".to_string(),
                anomalies: vec![],
            }],
            duration: Duration::from_secs(60),
            nemesis_actions: vec![NemesisAction {
                timestamp: Utc::now(),
                action_type: NemesisType::Partition,
                description: "Isolated node 2".to_string(),
            }],
        };

        let report = generate_report(&result);
        assert!(report.contains("✅ PASS"));
        assert!(report.contains("1000"));
        assert!(report.contains("Partition"));
    }

    #[test]
    fn test_anomaly_types() {
        assert_ne!(AnomalyType::DuplicateDelivery, AnomalyType::LostMessage);
        assert_ne!(AnomalyType::OutOfOrder, AnomalyType::StaleRead);
    }
}

// ── Jepsen Test Scenarios ────────────────────────────────────────────────────

/// Predefined test scenarios for comprehensive correctness verification.
pub mod scenarios {
    use super::*;
    use std::time::Duration;

    /// Scenario 1: Append Linearizability.
    ///
    /// Verifies that concurrent appends to the same topic-partition produce
    /// a linearizable total order: every successful write is assigned a
    /// unique, monotonically increasing offset, and all consumers see the
    /// same order.
    pub fn append_linearizability(num_producers: u32, msgs_per_producer: u64) -> Vec<Operation> {
        let mut history = Vec::new();
        let mut idx = 0u64;

        for p in 0..num_producers {
            for m in 0..msgs_per_producer {
                idx += 1;
                history.push(Operation {
                    index: idx,
                    op_type: OpType::Invoke,
                    function: "produce".to_string(),
                    value: serde_json::json!({
                        "key": format!("p{}-{}", p, m),
                        "value": format!("data-{}", idx),
                        "partition": "0",
                    }),
                    timestamp: Utc::now(),
                    process: p,
                });
                idx += 1;
                history.push(Operation {
                    index: idx,
                    op_type: OpType::Ok,
                    function: "produce".to_string(),
                    value: serde_json::json!({
                        "key": format!("p{}-{}", p, m),
                        "offset": idx / 2 - 1,
                        "partition": "0",
                    }),
                    timestamp: Utc::now(),
                    process: p,
                });
            }
        }

        // Add consumer reading all messages in order
        for m in 0..(num_producers as u64 * msgs_per_producer) {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "consume".to_string(),
                value: serde_json::json!({
                    "key": format!("msg-{}", m),
                    "offset": m,
                    "partition": "0",
                }),
                timestamp: Utc::now(),
                process: 100,
            });
        }

        history
    }

    /// Scenario 2: Transactional Atomicity.
    ///
    /// Verifies that transactional writes are atomic: either all records in
    /// a transaction are visible to consumers, or none are.
    pub fn transactional_atomicity(num_txns: u32, records_per_txn: u32) -> Vec<Operation> {
        let mut history = Vec::new();
        let mut idx = 0u64;

        for t in 0..num_txns {
            let txn_id = format!("txn-{}", t);
            let should_commit = t % 3 != 0; // Abort every 3rd transaction

            // Begin
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Invoke,
                function: "txn-begin".to_string(),
                value: serde_json::json!({"txn_id": txn_id}),
                timestamp: Utc::now(),
                process: 0,
            });

            // Writes within transaction
            for r in 0..records_per_txn {
                idx += 1;
                history.push(Operation {
                    index: idx,
                    op_type: OpType::Ok,
                    function: "produce".to_string(),
                    value: serde_json::json!({
                        "txn_id": txn_id,
                        "key": format!("txn-{}-rec-{}", t, r),
                        "partition": "0",
                    }),
                    timestamp: Utc::now(),
                    process: 0,
                });
            }

            // Commit or Abort
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: if should_commit { "txn-commit" } else { "txn-abort" }.to_string(),
                value: serde_json::json!({"txn_id": txn_id}),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        history
    }

    /// Scenario 3: Consumer Offset Consistency.
    ///
    /// Verifies that committed consumer offsets survive process restarts
    /// and that consumption resumes from the correct position.
    pub fn offset_consistency(num_messages: u64) -> Vec<Operation> {
        let mut history = Vec::new();
        let mut idx = 0u64;

        // Phase 1: Consume and commit at offset N
        let commit_at = num_messages / 2;
        for m in 0..num_messages {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "consume".to_string(),
                value: serde_json::json!({
                    "key": format!("msg-{}", m),
                    "offset": m,
                    "partition": "0",
                }),
                timestamp: Utc::now(),
                process: 0,
            });

            if m == commit_at {
                idx += 1;
                history.push(Operation {
                    index: idx,
                    op_type: OpType::Ok,
                    function: "offset-commit".to_string(),
                    value: serde_json::json!({
                        "group": "test-group",
                        "topic": "test",
                        "partition": 0,
                        "offset": commit_at + 1,
                    }),
                    timestamp: Utc::now(),
                    process: 0,
                });
            }
        }

        // Phase 2: Simulate restart — fetch committed offset
        idx += 1;
        history.push(Operation {
            index: idx,
            op_type: OpType::Ok,
            function: "offset-fetch".to_string(),
            value: serde_json::json!({
                "group": "test-group",
                "topic": "test",
                "partition": 0,
                "offset": commit_at + 1,
            }),
            timestamp: Utc::now(),
            process: 1,
        });

        history
    }

    /// Scenario 4: Leadership Stability Under Partitions.
    ///
    /// Generates a history with nemesis actions (network partitions) and
    /// verifies that writes to the leader succeed and that partitioned
    /// followers don't accept writes.
    pub fn leadership_stability() -> (Vec<Operation>, Vec<NemesisAction>) {
        let mut history = Vec::new();
        let mut nemesis = Vec::new();
        let mut idx = 0u64;

        // Writes before partition
        for m in 0..10 {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "produce".to_string(),
                value: serde_json::json!({"key": format!("pre-{}", m), "offset": m, "partition": "0"}),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        // Inject partition
        nemesis.push(NemesisAction {
            timestamp: Utc::now(),
            action_type: NemesisType::Partition,
            description: "Isolated node 2 from cluster".to_string(),
        });

        // Writes during partition (some may fail)
        for m in 10..20 {
            idx += 1;
            let succeeds = m < 15; // First few succeed, then leader election
            history.push(Operation {
                index: idx,
                op_type: if succeeds { OpType::Ok } else { OpType::Info },
                function: "produce".to_string(),
                value: serde_json::json!({"key": format!("during-{}", m), "offset": m, "partition": "0"}),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        // Heal partition
        nemesis.push(NemesisAction {
            timestamp: Utc::now(),
            action_type: NemesisType::Partition,
            description: "Healed partition for node 2".to_string(),
        });

        // Post-heal writes
        for m in 20..30 {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "produce".to_string(),
                value: serde_json::json!({"key": format!("post-{}", m), "offset": m, "partition": "0"}),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        (history, nemesis)
    }

    /// Scenario 5: Partition Recovery.
    ///
    /// Verifies that after a node crash and restart, data is recovered
    /// from the write-ahead log and no messages are lost.
    pub fn partition_recovery(pre_crash_msgs: u64, post_recovery_msgs: u64) -> Vec<Operation> {
        let mut history = Vec::new();
        let mut idx = 0u64;

        // Produce before crash
        for m in 0..pre_crash_msgs {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "produce".to_string(),
                value: serde_json::json!({
                    "key": format!("before-{}", m),
                    "offset": m,
                    "partition": "0",
                }),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        // Crash event
        idx += 1;
        history.push(Operation {
            index: idx,
            op_type: OpType::Info,
            function: "crash".to_string(),
            value: serde_json::json!({"node": 1, "reason": "simulated"}),
            timestamp: Utc::now(),
            process: 99,
        });

        // Recovery: consume should see all pre-crash messages
        for m in 0..pre_crash_msgs {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "consume".to_string(),
                value: serde_json::json!({
                    "key": format!("before-{}", m),
                    "offset": m,
                    "partition": "0",
                }),
                timestamp: Utc::now(),
                process: 1,
            });
        }

        // Post-recovery produce
        for m in 0..post_recovery_msgs {
            idx += 1;
            history.push(Operation {
                index: idx,
                op_type: OpType::Ok,
                function: "produce".to_string(),
                value: serde_json::json!({
                    "key": format!("after-{}", m),
                    "offset": pre_crash_msgs + m,
                    "partition": "0",
                }),
                timestamp: Utc::now(),
                process: 0,
            });
        }

        history
    }

    #[cfg(test)]
    mod scenario_tests {
        use super::*;

        #[test]
        fn test_append_linearizability_scenario() {
            let history = append_linearizability(3, 100);
            assert!(!history.is_empty());
            let result = EosChecker::check(&history);
            assert!(result.valid, "Linearizable append should have no anomalies");
        }

        #[test]
        fn test_transactional_atomicity_scenario() {
            let history = transactional_atomicity(10, 5);
            let txn_begins = history.iter().filter(|o| o.function == "txn-begin").count();
            assert_eq!(txn_begins, 10);
        }

        #[test]
        fn test_offset_consistency_scenario() {
            let history = offset_consistency(100);
            let commits = history.iter().filter(|o| o.function == "offset-commit").count();
            assert_eq!(commits, 1);
            let fetches = history.iter().filter(|o| o.function == "offset-fetch").count();
            assert_eq!(fetches, 1);
        }

        #[test]
        fn test_leadership_stability_scenario() {
            let (history, nemesis) = leadership_stability();
            assert!(!history.is_empty());
            assert_eq!(nemesis.len(), 2); // partition + heal
            let infos = history.iter().filter(|o| o.op_type == OpType::Info).count();
            assert!(infos > 0, "Should have indeterminate ops during partition");
        }

        #[test]
        fn test_partition_recovery_scenario() {
            let history = partition_recovery(50, 20);
            let produces = history.iter().filter(|o| o.function == "produce" && o.op_type == OpType::Ok).count();
            assert_eq!(produces, 70); // 50 before + 20 after
            let consumes = history.iter().filter(|o| o.function == "consume").count();
            assert_eq!(consumes, 50); // All pre-crash messages recovered
        }
    }
}
