//! Contract test runner — executes contracts against an embedded Streamline instance.

use super::assertion::{AssertionEngine, AssertionResult};
use super::definition::StreamContract;
use super::mock_producer::{MockProducer, MockProducerConfig};
use crate::embedded::EmbeddedStreamline;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Instant;

/// Configuration for the contract runner.
#[derive(Debug, Clone)]
pub struct ContractRunnerConfig {
    /// Data directory (None = in-memory)
    pub data_dir: Option<PathBuf>,
    /// Whether to produce mock data before testing
    pub produce_mock_data: bool,
    /// Maximum messages to read for validation
    pub max_validation_messages: usize,
    /// Output format for reports
    pub output_format: ReportFormat,
}

/// Report output format.
#[derive(Debug, Clone, Default)]
pub enum ReportFormat {
    #[default]
    Text,
    Json,
    JunitXml,
}

impl Default for ContractRunnerConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            produce_mock_data: true,
            max_validation_messages: 10_000,
            output_format: ReportFormat::Text,
        }
    }
}

/// The outcome of a single test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestOutcome {
    Passed,
    Failed,
    Skipped,
    Error(String),
}

/// Result of running a single contract test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Contract name
    pub contract_name: String,
    /// Topic tested
    pub topic: String,
    /// Overall outcome
    pub outcome: TestOutcome,
    /// Individual assertion results
    pub assertions: Vec<AssertionResult>,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Messages produced (if mock data was used)
    pub messages_produced: u64,
    /// Messages validated
    pub messages_validated: u64,
}

/// A complete test suite with multiple contract results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSuite {
    /// Suite name
    pub name: String,
    /// Individual test results
    pub results: Vec<TestResult>,
    /// Total duration in milliseconds
    pub total_duration_ms: u64,
    /// Summary counts
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errors: usize,
}

/// A test report with formatted output.
#[derive(Debug, Clone)]
pub struct TestReport {
    pub suite: TestSuite,
}

impl TestReport {
    /// Format the report as text.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "\n=== Stream Contract Test Suite: {} ===\n\n",
            self.suite.name
        ));

        for result in &self.suite.results {
            let status = match &result.outcome {
                TestOutcome::Passed => "✓ PASS",
                TestOutcome::Failed => "✗ FAIL",
                TestOutcome::Skipped => "- SKIP",
                TestOutcome::Error(e) => &format!("! ERR: {}", e),
            };
            out.push_str(&format!(
                "  {} {} ({}ms, {} messages)\n",
                status, result.contract_name, result.duration_ms, result.messages_validated
            ));

            for assertion in &result.assertions {
                if !assertion.passed {
                    out.push_str(&format!(
                        "      ✗ {}: {}\n",
                        assertion.name, assertion.message
                    ));
                    for detail in &assertion.failure_details {
                        out.push_str(&format!("        - {}\n", detail));
                    }
                }
            }
        }

        out.push_str(&format!(
            "\nResults: {} passed, {} failed, {} skipped, {} errors ({}ms total)\n",
            self.suite.passed,
            self.suite.failed,
            self.suite.skipped,
            self.suite.errors,
            self.suite.total_duration_ms,
        ));
        out
    }

    /// Format the report as JUnit XML.
    pub fn to_junit_xml(&self) -> String {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str(&format!(
            "<testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" errors=\"{}\" time=\"{:.3}\">\n",
            self.suite.name,
            self.suite.results.len(),
            self.suite.failed,
            self.suite.errors,
            self.suite.total_duration_ms as f64 / 1000.0,
        ));

        for result in &self.suite.results {
            xml.push_str(&format!(
                "  <testcase name=\"{}\" classname=\"{}\" time=\"{:.3}\"",
                result.contract_name,
                result.topic,
                result.duration_ms as f64 / 1000.0,
            ));

            match &result.outcome {
                TestOutcome::Failed => {
                    xml.push_str(">\n");
                    let failure_msg: String = result
                        .assertions
                        .iter()
                        .filter(|a| !a.passed)
                        .map(|a| a.message.clone())
                        .collect::<Vec<_>>()
                        .join("; ");
                    xml.push_str(&format!(
                        "    <failure message=\"{}\">{}</failure>\n",
                        failure_msg, failure_msg
                    ));
                    xml.push_str("  </testcase>\n");
                }
                TestOutcome::Error(e) => {
                    xml.push_str(">\n");
                    xml.push_str(&format!("    <error message=\"{}\">{}</error>\n", e, e));
                    xml.push_str("  </testcase>\n");
                }
                TestOutcome::Skipped => {
                    xml.push_str(">\n    <skipped/>\n  </testcase>\n");
                }
                TestOutcome::Passed => {
                    xml.push_str("/>\n");
                }
            }
        }

        xml.push_str("</testsuite>\n");
        xml
    }
}

/// Runs stream contracts against an embedded Streamline instance.
pub struct ContractRunner {
    config: ContractRunnerConfig,
}

impl ContractRunner {
    /// Create a new contract runner.
    pub fn new(config: ContractRunnerConfig) -> Self {
        Self { config }
    }

    /// Run a set of contracts and return the test suite.
    pub fn run(&self, contracts: &[StreamContract]) -> Result<TestSuite> {
        let suite_start = Instant::now();
        let instance = EmbeddedStreamline::in_memory()?;
        let mut results = Vec::new();

        for contract in contracts {
            let result = self.run_contract(&instance, contract);
            results.push(result);
        }

        let passed = results
            .iter()
            .filter(|r| matches!(r.outcome, TestOutcome::Passed))
            .count();
        let failed = results
            .iter()
            .filter(|r| matches!(r.outcome, TestOutcome::Failed))
            .count();
        let skipped = results
            .iter()
            .filter(|r| matches!(r.outcome, TestOutcome::Skipped))
            .count();
        let errors = results
            .iter()
            .filter(|r| matches!(r.outcome, TestOutcome::Error(_)))
            .count();

        Ok(TestSuite {
            name: "StreamContracts".to_string(),
            results,
            total_duration_ms: suite_start.elapsed().as_millis() as u64,
            passed,
            failed,
            skipped,
            errors,
        })
    }

    fn run_contract(&self, instance: &EmbeddedStreamline, contract: &StreamContract) -> TestResult {
        let start = Instant::now();
        let mut messages_produced = 0u64;

        // Create topic
        if let Err(e) = instance.create_topic(&contract.topic, 1) {
            if !e.to_string().contains("already exists") {
                return TestResult {
                    contract_name: contract.name.clone(),
                    topic: contract.topic.clone(),
                    outcome: TestOutcome::Error(format!("Failed to create topic: {}", e)),
                    assertions: Vec::new(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    messages_produced: 0,
                    messages_validated: 0,
                };
            }
        }

        // Produce mock data if configured
        if self.config.produce_mock_data {
            if let Some(ref mock_spec) = contract.spec.mock_data {
                let producer = MockProducer::new(MockProducerConfig {
                    count: mock_spec.count,
                    template: mock_spec.template.clone(),
                    key_template: mock_spec.key_template.clone(),
                    partition: mock_spec.partition,
                    delay_ms: None,
                });

                match producer.generate() {
                    Ok(messages) => {
                        for msg in &messages {
                            if let Err(e) = instance.produce(
                                &contract.topic,
                                msg.partition,
                                msg.key.clone(),
                                msg.value.clone(),
                            ) {
                                return TestResult {
                                    contract_name: contract.name.clone(),
                                    topic: contract.topic.clone(),
                                    outcome: TestOutcome::Error(format!("Produce failed: {}", e)),
                                    assertions: Vec::new(),
                                    duration_ms: start.elapsed().as_millis() as u64,
                                    messages_produced,
                                    messages_validated: 0,
                                };
                            }
                            messages_produced += 1;
                        }
                    }
                    Err(e) => {
                        return TestResult {
                            contract_name: contract.name.clone(),
                            topic: contract.topic.clone(),
                            outcome: TestOutcome::Error(format!("Mock generation failed: {}", e)),
                            assertions: Vec::new(),
                            duration_ms: start.elapsed().as_millis() as u64,
                            messages_produced: 0,
                            messages_validated: 0,
                        };
                    }
                }
            }
        }

        // Read messages back and validate
        let records =
            match instance.consume(&contract.topic, 0, 0, self.config.max_validation_messages) {
                Ok(records) => records,
                Err(e) => {
                    return TestResult {
                        contract_name: contract.name.clone(),
                        topic: contract.topic.clone(),
                        outcome: TestOutcome::Error(format!("Consume failed: {}", e)),
                        assertions: Vec::new(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        messages_produced,
                        messages_validated: 0,
                    };
                }
            };

        // Parse records as JSON for validation
        let json_messages: Vec<serde_json::Value> = records
            .iter()
            .filter_map(|r| serde_json::from_slice(&r.value).ok())
            .collect();

        let messages_validated = json_messages.len() as u64;

        // Run assertions
        let engine = AssertionEngine::new(contract.clone());
        let assertions = engine.validate_messages(&json_messages);

        let all_passed = assertions.iter().all(|a| a.passed);
        let outcome = if all_passed {
            TestOutcome::Passed
        } else {
            TestOutcome::Failed
        };

        TestResult {
            contract_name: contract.name.clone(),
            topic: contract.topic.clone(),
            outcome,
            assertions,
            duration_ms: start.elapsed().as_millis() as u64,
            messages_produced,
            messages_validated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::definition::*;

    #[test]
    fn test_contract_runner_passing() {
        let contract = StreamContract::new("user-events", "test-topic")
            .with_field("user_id", FieldType::String)
            .with_field("count", FieldType::Integer)
            .with_mock_data(10, r#"{"user_id":"{{uuid}}","count":{{i}}}"#);

        let runner = ContractRunner::new(ContractRunnerConfig::default());
        let suite = runner.run(&[contract]).unwrap();

        assert_eq!(suite.passed, 1);
        assert_eq!(suite.failed, 0);
    }

    #[test]
    fn test_contract_runner_failing() {
        let contract = StreamContract::new("bad-contract", "test-topic-fail")
            .with_field("missing_field", FieldType::String)
            .with_mock_data(5, r#"{"other_field":"value"}"#);

        let runner = ContractRunner::new(ContractRunnerConfig::default());
        let suite = runner.run(&[contract]).unwrap();

        assert_eq!(suite.failed, 1);
        assert_eq!(suite.passed, 0);
    }

    #[test]
    fn test_junit_xml_output() {
        let contract = StreamContract::new("xml-test", "xml-topic")
            .with_field("id", FieldType::String)
            .with_mock_data(3, r#"{"id":"{{uuid}}"}"#);

        let runner = ContractRunner::new(ContractRunnerConfig::default());
        let suite = runner.run(&[contract]).unwrap();
        let report = TestReport { suite };
        let xml = report.to_junit_xml();
        assert!(xml.contains("<testsuite"));
        assert!(xml.contains("xml-test"));
    }
}
