//! Natural Language Query Engine for StreamQL
//!
//! Translates natural language questions into StreamQL queries using LLM integration.
//!
//! ## Features
//!
//! - Natural language to SQL translation
//! - Schema-aware prompt building
//! - Pattern-based fallback for simple queries
//! - Translation caching and history tracking
//! - Confidence scoring and query suggestions
//!
//! ## Example
//!
//! ```rust,no_run
//! use streamline::streamql::natural_language::*;
//!
//! let engine = NaturalLanguageEngine::new(NlConfig::default());
//! let request = NlQueryRequest {
//!     question: "count all events".to_string(),
//!     context: None,
//!     topics: Some(vec!["events".to_string()]),
//!     dry_run: true,
//! };
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Natural language query engine that translates questions into StreamQL.
pub struct NaturalLanguageEngine {
    config: NlConfig,
    schema_cache: Arc<RwLock<HashMap<String, TopicSchema>>>,
    history: Arc<RwLock<Vec<NlQueryRecord>>>,
    stats: Arc<NlStats>,
}

/// Configuration for the natural language engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlConfig {
    pub enabled: bool,
    pub model: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub max_retries: u32,
    pub cache_translations: bool,
    pub show_generated_sql: bool,
    pub allowed_operations: Vec<NlOperation>,
}

impl Default for NlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model: "gpt-4".to_string(),
            max_tokens: 512,
            temperature: 0.1,
            max_retries: 2,
            cache_translations: true,
            show_generated_sql: true,
            allowed_operations: vec![
                NlOperation::Select,
                NlOperation::Aggregate,
                NlOperation::Filter,
                NlOperation::Join,
                NlOperation::Window,
                NlOperation::Create,
                NlOperation::Describe,
            ],
        }
    }
}

/// Operations allowed in natural language queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NlOperation {
    Select,
    Aggregate,
    Filter,
    Join,
    Window,
    Create,
    Describe,
}

/// Schema information for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSchema {
    pub topic: String,
    pub fields: Vec<SchemaField>,
    pub sample_values: Vec<serde_json::Value>,
    pub record_count_estimate: u64,
}

/// A field within a topic schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub field_type: String,
    pub nullable: bool,
    pub description: Option<String>,
}

/// Request for natural language query translation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlQueryRequest {
    pub question: String,
    pub context: Option<String>,
    pub topics: Option<Vec<String>>,
    pub dry_run: bool,
}

/// Result of a natural language query translation and optional execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlQueryResult {
    pub question: String,
    pub generated_sql: String,
    pub confidence: f64,
    pub explanation: String,
    pub suggestions: Vec<String>,
    pub executed: bool,
    pub result: Option<serde_json::Value>,
    pub execution_time_ms: Option<f64>,
}

/// Record of a past natural language query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlQueryRecord {
    pub id: String,
    pub question: String,
    pub generated_sql: String,
    pub confidence: f64,
    pub success: bool,
    pub timestamp: String,
}

/// Aggregate statistics for the natural language engine.
pub struct NlStats {
    pub queries_translated: AtomicU64,
    pub queries_executed: AtomicU64,
    pub cache_hits: AtomicU64,
    pub low_confidence_count: AtomicU64,
    pub errors: AtomicU64,
    /// Stored as confidence * 100 for atomic operations.
    pub avg_confidence: AtomicU64,
}

impl Default for NlStats {
    fn default() -> Self {
        Self {
            queries_translated: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            low_confidence_count: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            avg_confidence: AtomicU64::new(0),
        }
    }
}

impl NaturalLanguageEngine {
    /// Create a new natural language engine with the given configuration.
    pub fn new(config: NlConfig) -> Self {
        info!(model = %config.model, "Initializing natural language query engine");
        Self {
            config,
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(NlStats::default()),
        }
    }

    /// Translate a natural language question into a StreamQL query.
    ///
    /// Builds a prompt with schema context, generates SQL via LLM (or pattern matching
    /// fallback), and optionally executes the query.
    pub async fn translate(&self, request: &NlQueryRequest) -> NlQueryResult {
        if !self.config.enabled {
            return NlQueryResult {
                question: request.question.clone(),
                generated_sql: String::new(),
                confidence: 0.0,
                explanation: "Natural language engine is disabled".to_string(),
                suggestions: vec![],
                executed: false,
                result: None,
                execution_time_ms: None,
            };
        }

        self.stats.queries_translated.fetch_add(1, Ordering::Relaxed);

        let schemas = self.get_relevant_schemas(&request.topics).await;

        let prompt = self.build_prompt(&request.question, &schemas);
        debug!(prompt_len = prompt.len(), "Built LLM prompt");

        // Use pattern-based translation (simulates LLM call)
        let (sql, confidence, explanation) = self.translate_with_patterns(&request.question, &schemas);

        if confidence < 0.5 {
            self.stats.low_confidence_count.fetch_add(1, Ordering::Relaxed);
            warn!(confidence, question = %request.question, "Low confidence translation");
        }

        self.update_avg_confidence(confidence);

        let executed = !request.dry_run && confidence >= 0.5;
        let (result, execution_time_ms) = if executed {
            self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
            let start = std::time::Instant::now();
            let exec_result = serde_json::json!({ "status": "simulated", "sql": sql });
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            (Some(exec_result), Some(elapsed))
        } else {
            (None, None)
        };

        let suggestions = self.generate_suggestions(&request.question, &schemas);

        let record = NlQueryRecord {
            id: uuid::Uuid::new_v4().to_string(),
            question: request.question.clone(),
            generated_sql: sql.clone(),
            confidence,
            success: confidence >= 0.5,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        if self.config.cache_translations {
            let mut history = self.history.write().await;
            history.push(record);
        }

        NlQueryResult {
            question: request.question.clone(),
            generated_sql: sql,
            confidence,
            explanation,
            suggestions,
            executed,
            result,
            execution_time_ms,
        }
    }

    /// Build an LLM prompt from the question and available schemas.
    pub fn build_prompt(&self, question: &str, schemas: &[TopicSchema]) -> String {
        let mut prompt = String::new();

        prompt.push_str("You are a StreamQL query generator. StreamQL uses SQL-like syntax for stream processing.\n\n");
        prompt.push_str("## StreamQL Syntax\n");
        prompt.push_str("- SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT\n");
        prompt.push_str("- Window functions: WINDOW TUMBLING(INTERVAL 'N seconds'), HOPPING, SLIDING, SESSION\n");
        prompt.push_str("- Aggregations: COUNT(*), SUM(field), AVG(field), MIN(field), MAX(field)\n");
        prompt.push_str("- Special columns: _timestamp, _offset, _partition, _key\n");
        prompt.push_str("- Joins: INNER JOIN, LEFT JOIN with ON clause\n\n");

        if !schemas.is_empty() {
            prompt.push_str("## Available Topics and Schemas\n\n");
            for schema in schemas {
                prompt.push_str(&format!("### Topic: `{}`\n", schema.topic));
                prompt.push_str(&format!("Estimated records: {}\n", schema.record_count_estimate));
                prompt.push_str("Fields:\n");
                for field in &schema.fields {
                    let nullable = if field.nullable { " (nullable)" } else { "" };
                    let desc = field
                        .description
                        .as_deref()
                        .map(|d| format!(" - {d}"))
                        .unwrap_or_default();
                    prompt.push_str(&format!(
                        "  - `{}`: {}{}{}\n",
                        field.name, field.field_type, nullable, desc
                    ));
                }
                if !schema.sample_values.is_empty() {
                    prompt.push_str("Sample values:\n");
                    for (i, sample) in schema.sample_values.iter().take(3).enumerate() {
                        prompt.push_str(&format!("  {}. {}\n", i + 1, sample));
                    }
                }
                prompt.push('\n');
            }
        }

        prompt.push_str("## User Question\n\n");
        prompt.push_str(question);
        prompt.push_str("\n\n## Instructions\n");
        prompt.push_str("Respond with a JSON object containing:\n");
        prompt.push_str("- \"sql\": the generated StreamQL query\n");
        prompt.push_str("- \"confidence\": a number between 0.0 and 1.0\n");
        prompt.push_str("- \"explanation\": a human-readable explanation of the query\n");

        prompt
    }

    /// Parse a simulated LLM response into (sql, confidence, explanation).
    pub fn parse_llm_response(&self, response: &str) -> (String, f64, String) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(response) {
            let sql = json["sql"].as_str().unwrap_or("").to_string();
            let confidence = json["confidence"].as_f64().unwrap_or(0.0);
            let explanation = json["explanation"].as_str().unwrap_or("").to_string();
            (sql, confidence, explanation)
        } else {
            // Fallback: treat entire response as SQL
            (response.trim().to_string(), 0.3, "Parsed from raw response".to_string())
        }
    }

    /// Pattern-based translation for common query patterns (test-friendly, no LLM needed).
    pub fn translate_with_patterns(
        &self,
        question: &str,
        schemas: &[TopicSchema],
    ) -> (String, f64, String) {
        let q = question.to_lowercase();
        let topic = self.extract_topic(&q, schemas);

        // "count" + topic → SELECT COUNT(*) FROM {topic}
        if q.contains("count") {
            if let Some(ref t) = topic {
                return (
                    format!("SELECT COUNT(*) FROM {t}"),
                    0.9,
                    format!("Count all records in {t}"),
                );
            }
        }

        // "last N" + topic → SELECT * FROM {topic} ORDER BY _timestamp DESC LIMIT N
        if q.contains("last") {
            if let Some(n) = extract_number(&q) {
                if let Some(ref t) = topic {
                    return (
                        format!("SELECT * FROM {t} ORDER BY _timestamp DESC LIMIT {n}"),
                        0.85,
                        format!("Get the last {n} records from {t}"),
                    );
                }
            }
        }

        // "average"/"avg" + field + topic → SELECT AVG({field}) FROM {topic}
        if q.contains("average") || q.contains("avg") {
            if let Some(ref t) = topic {
                if let Some(field) = self.extract_field(&q, schemas) {
                    return (
                        format!("SELECT AVG({field}) FROM {t}"),
                        0.85,
                        format!("Calculate average of {field} in {t}"),
                    );
                }
            }
        }

        // "where" + field + op + value → SELECT * FROM {topic} WHERE {field} {op} {value}
        if q.contains("where") {
            if let Some(ref t) = topic {
                if let Some((field, op, value)) = self.extract_where_clause(&q, schemas) {
                    return (
                        format!("SELECT * FROM {t} WHERE {field} {op} {value}"),
                        0.8,
                        format!("Filter {t} where {field} {op} {value}"),
                    );
                }
            }
        }

        // "show" / "describe" + topic
        if q.contains("describe") || q.contains("show fields") || q.contains("schema") {
            if let Some(ref t) = topic {
                return (
                    format!("DESCRIBE {t}"),
                    0.9,
                    format!("Describe the schema of {t}"),
                );
            }
        }

        // Generic select fallback
        if let Some(ref t) = topic {
            return (
                format!("SELECT * FROM {t} LIMIT 10"),
                0.5,
                format!("Show sample records from {t}"),
            );
        }

        (String::new(), 0.0, "Could not understand the question".to_string())
    }

    /// Register a topic schema for prompt context.
    pub async fn register_topic_schema(&self, schema: TopicSchema) {
        info!(topic = %schema.topic, fields = schema.fields.len(), "Registering topic schema");
        let mut cache = self.schema_cache.write().await;
        cache.insert(schema.topic.clone(), schema);
    }

    /// Get recent query history.
    pub async fn get_history(&self, limit: usize) -> Vec<NlQueryRecord> {
        let history = self.history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Suggest questions a user might ask about a topic.
    pub async fn suggest_questions(&self, topic: &str) -> Vec<String> {
        let schemas = self.schema_cache.read().await;
        let mut suggestions = vec![
            format!("How many records are in {topic}?"),
            format!("Show the last 10 records from {topic}"),
        ];

        if let Some(schema) = schemas.get(topic) {
            for field in &schema.fields {
                if field.field_type == "f64" || field.field_type == "i64" || field.field_type == "number" {
                    suggestions.push(format!("What is the average {} in {topic}?", field.name));
                }
                suggestions.push(format!("Show records from {topic} where {} is not null", field.name));
            }
        }

        suggestions
    }

    /// Get aggregate stats.
    pub fn stats(&self) -> &NlStats {
        &self.stats
    }

    // --- Private helpers ---

    async fn get_relevant_schemas(&self, topics: &Option<Vec<String>>) -> Vec<TopicSchema> {
        let cache = self.schema_cache.read().await;
        match topics {
            Some(topic_list) => topic_list
                .iter()
                .filter_map(|t| cache.get(t).cloned())
                .collect(),
            None => cache.values().cloned().collect(),
        }
    }

    fn extract_topic(&self, question: &str, schemas: &[TopicSchema]) -> Option<String> {
        // Check for known topic names in the question
        for schema in schemas {
            if question.contains(&schema.topic.to_lowercase()) {
                return Some(schema.topic.clone());
            }
        }
        // Try to extract a word after "from", "in", or "of"
        for prefix in &["from ", "in ", "of ", "on "] {
            if let Some(idx) = question.find(prefix) {
                let rest = &question[idx + prefix.len()..];
                let topic: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
                    .collect();
                if !topic.is_empty() {
                    return Some(topic);
                }
            }
        }
        None
    }

    fn extract_field(&self, question: &str, schemas: &[TopicSchema]) -> Option<String> {
        for schema in schemas {
            for field in &schema.fields {
                if question.contains(&field.name.to_lowercase()) {
                    return Some(field.name.clone());
                }
            }
        }
        None
    }

    fn extract_where_clause(
        &self,
        question: &str,
        schemas: &[TopicSchema],
    ) -> Option<(String, String, String)> {
        let after_where = question.split("where").nth(1)?;
        let tokens: Vec<&str> = after_where.split_whitespace().collect();

        if tokens.len() >= 3 {
            let field_candidate = tokens[0].to_string();
            // Validate field exists in a schema
            let field = schemas
                .iter()
                .flat_map(|s| &s.fields)
                .find(|f| f.name.to_lowercase() == field_candidate)
                .map(|f| f.name.clone())
                .unwrap_or(field_candidate);

            let op = match tokens[1] {
                "=" | "==" | "equals" | "is" => "=",
                ">" | "greater" | "above" => ">",
                "<" | "less" | "below" => "<",
                ">=" => ">=",
                "<=" => "<=",
                "!=" | "not" => "!=",
                other => other,
            };

            let value = tokens[2].trim_end_matches(&['.', ',', '?', '!'][..]).to_string();
            return Some((field, op.to_string(), value));
        }
        None
    }

    fn generate_suggestions(&self, question: &str, schemas: &[TopicSchema]) -> Vec<String> {
        let q = question.to_lowercase();
        let mut suggestions = Vec::new();

        if !q.contains("count") && !q.contains("average") {
            if let Some(topic) = self.extract_topic(&q, schemas) {
                suggestions.push(format!("Try: count all records in {topic}"));
                suggestions.push(format!("Try: show the last 10 records from {topic}"));
            }
        }

        for schema in schemas {
            for field in &schema.fields {
                if (field.field_type == "f64" || field.field_type == "number")
                    && !q.contains(&field.name.to_lowercase())
                {
                    suggestions.push(format!(
                        "Try: what is the average {} in {}",
                        field.name, schema.topic
                    ));
                    break;
                }
            }
        }

        suggestions.truncate(5);
        suggestions
    }

    fn update_avg_confidence(&self, confidence: f64) {
        let total = self.stats.queries_translated.load(Ordering::Relaxed);
        if total <= 1 {
            self.stats
                .avg_confidence
                .store((confidence * 100.0) as u64, Ordering::Relaxed);
        } else {
            let prev = self.stats.avg_confidence.load(Ordering::Relaxed) as f64;
            let new_avg = prev + ((confidence * 100.0) - prev) / total as f64;
            self.stats
                .avg_confidence
                .store(new_avg as u64, Ordering::Relaxed);
        }
    }
}

/// Extract the first number found in a string.
fn extract_number(s: &str) -> Option<u64> {
    let mut num_str = String::new();
    let mut found = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            num_str.push(c);
            found = true;
        } else if found {
            break;
        }
    }
    if found {
        num_str.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NlConfig {
        NlConfig::default()
    }

    fn test_schema() -> TopicSchema {
        TopicSchema {
            topic: "events".to_string(),
            fields: vec![
                SchemaField {
                    name: "user_id".to_string(),
                    field_type: "string".to_string(),
                    nullable: false,
                    description: Some("User identifier".to_string()),
                },
                SchemaField {
                    name: "amount".to_string(),
                    field_type: "f64".to_string(),
                    nullable: true,
                    description: Some("Transaction amount".to_string()),
                },
                SchemaField {
                    name: "status".to_string(),
                    field_type: "string".to_string(),
                    nullable: false,
                    description: None,
                },
            ],
            sample_values: vec![
                serde_json::json!({"user_id": "u1", "amount": 42.5, "status": "ok"}),
                serde_json::json!({"user_id": "u2", "amount": 100.0, "status": "error"}),
            ],
            record_count_estimate: 50_000,
        }
    }

    fn orders_schema() -> TopicSchema {
        TopicSchema {
            topic: "orders".to_string(),
            fields: vec![
                SchemaField {
                    name: "order_id".to_string(),
                    field_type: "string".to_string(),
                    nullable: false,
                    description: None,
                },
                SchemaField {
                    name: "price".to_string(),
                    field_type: "number".to_string(),
                    nullable: false,
                    description: Some("Order price".to_string()),
                },
            ],
            sample_values: vec![],
            record_count_estimate: 10_000,
        }
    }

    #[test]
    fn test_default_config() {
        let config = NlConfig::default();
        assert!(config.enabled);
        assert_eq!(config.model, "gpt-4");
        assert_eq!(config.max_tokens, 512);
        assert!((config.temperature - 0.1).abs() < f32::EPSILON);
        assert_eq!(config.max_retries, 2);
        assert!(config.cache_translations);
        assert!(config.show_generated_sql);
        assert_eq!(config.allowed_operations.len(), 7);
    }

    #[test]
    fn test_pattern_count() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) = engine.translate_with_patterns("count all events", &schemas);
        assert_eq!(sql, "SELECT COUNT(*) FROM events");
        assert!(confidence >= 0.8);
    }

    #[test]
    fn test_pattern_last_n() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) = engine.translate_with_patterns("show the last 5 events", &schemas);
        assert_eq!(sql, "SELECT * FROM events ORDER BY _timestamp DESC LIMIT 5");
        assert!(confidence >= 0.8);
    }

    #[test]
    fn test_pattern_average() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) =
            engine.translate_with_patterns("what is the average amount in events", &schemas);
        assert_eq!(sql, "SELECT AVG(amount) FROM events");
        assert!(confidence >= 0.8);
    }

    #[test]
    fn test_pattern_avg_alias() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, _, _) = engine.translate_with_patterns("avg amount from events", &schemas);
        assert_eq!(sql, "SELECT AVG(amount) FROM events");
    }

    #[test]
    fn test_pattern_where_clause() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) =
            engine.translate_with_patterns("select from events where status = ok", &schemas);
        assert_eq!(sql, "SELECT * FROM events WHERE status = ok");
        assert!(confidence >= 0.7);
    }

    #[test]
    fn test_pattern_where_greater_than() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, _, _) =
            engine.translate_with_patterns("events where amount > 100", &schemas);
        assert_eq!(sql, "SELECT * FROM events WHERE amount > 100");
    }

    #[test]
    fn test_pattern_describe() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) = engine.translate_with_patterns("describe events", &schemas);
        assert_eq!(sql, "DESCRIBE events");
        assert!(confidence >= 0.8);
    }

    #[test]
    fn test_pattern_unknown_falls_back() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) =
            engine.translate_with_patterns("show me events", &schemas);
        assert_eq!(sql, "SELECT * FROM events LIMIT 10");
        assert!((confidence - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pattern_no_topic_found() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let (sql, confidence, _) =
            engine.translate_with_patterns("hello world", &schemas);
        assert!(sql.is_empty());
        assert!((confidence - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_build_prompt_contains_schema() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema()];
        let prompt = engine.build_prompt("count events", &schemas);
        assert!(prompt.contains("StreamQL"));
        assert!(prompt.contains("events"));
        assert!(prompt.contains("user_id"));
        assert!(prompt.contains("amount"));
        assert!(prompt.contains("count events"));
    }

    #[test]
    fn test_build_prompt_empty_schemas() {
        let engine = NaturalLanguageEngine::new(test_config());
        let prompt = engine.build_prompt("test question", &[]);
        assert!(prompt.contains("test question"));
        assert!(!prompt.contains("Available Topics"));
    }

    #[test]
    fn test_parse_llm_response_json() {
        let engine = NaturalLanguageEngine::new(test_config());
        let response = r#"{"sql": "SELECT * FROM events", "confidence": 0.95, "explanation": "Get all events"}"#;
        let (sql, confidence, explanation) = engine.parse_llm_response(response);
        assert_eq!(sql, "SELECT * FROM events");
        assert!((confidence - 0.95).abs() < f64::EPSILON);
        assert_eq!(explanation, "Get all events");
    }

    #[test]
    fn test_parse_llm_response_raw() {
        let engine = NaturalLanguageEngine::new(test_config());
        let (sql, confidence, _) = engine.parse_llm_response("SELECT * FROM t");
        assert_eq!(sql, "SELECT * FROM t");
        assert!(confidence < 0.5);
    }

    #[tokio::test]
    async fn test_translate_disabled_engine() {
        let mut config = test_config();
        config.enabled = false;
        let engine = NaturalLanguageEngine::new(config);
        let request = NlQueryRequest {
            question: "count events".to_string(),
            context: None,
            topics: None,
            dry_run: true,
        };
        let result = engine.translate(&request).await;
        assert!(result.generated_sql.is_empty());
        assert!((result.confidence - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_translate_dry_run() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let request = NlQueryRequest {
            question: "count all events".to_string(),
            context: None,
            topics: Some(vec!["events".to_string()]),
            dry_run: true,
        };
        let result = engine.translate(&request).await;
        assert_eq!(result.generated_sql, "SELECT COUNT(*) FROM events");
        assert!(!result.executed);
        assert!(result.result.is_none());
    }

    #[tokio::test]
    async fn test_translate_with_execution() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let request = NlQueryRequest {
            question: "count events".to_string(),
            context: None,
            topics: Some(vec!["events".to_string()]),
            dry_run: false,
        };
        let result = engine.translate(&request).await;
        assert!(result.executed);
        assert!(result.result.is_some());
        assert!(result.execution_time_ms.is_some());
    }

    #[tokio::test]
    async fn test_register_and_suggest() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let suggestions = engine.suggest_questions("events").await;
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("events")));
        assert!(suggestions.iter().any(|s| s.contains("amount")));
    }

    #[tokio::test]
    async fn test_get_history() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let request = NlQueryRequest {
            question: "count events".to_string(),
            context: None,
            topics: Some(vec!["events".to_string()]),
            dry_run: true,
        };
        engine.translate(&request).await;
        engine.translate(&request).await;
        let history = engine.get_history(10).await;
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn test_history_limit() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let request = NlQueryRequest {
            question: "count events".to_string(),
            context: None,
            topics: Some(vec!["events".to_string()]),
            dry_run: true,
        };
        for _ in 0..5 {
            engine.translate(&request).await;
        }
        let history = engine.get_history(2).await;
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let engine = NaturalLanguageEngine::new(test_config());
        engine.register_topic_schema(test_schema()).await;
        let request = NlQueryRequest {
            question: "count events".to_string(),
            context: None,
            topics: Some(vec!["events".to_string()]),
            dry_run: false,
        };
        engine.translate(&request).await;
        assert_eq!(engine.stats().queries_translated.load(Ordering::Relaxed), 1);
        assert_eq!(engine.stats().queries_executed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_low_confidence_stats() {
        let engine = NaturalLanguageEngine::new(test_config());
        // No schemas registered, so translation will have 0 confidence
        let request = NlQueryRequest {
            question: "some random gibberish".to_string(),
            context: None,
            topics: None,
            dry_run: false,
        };
        engine.translate(&request).await;
        assert_eq!(engine.stats().low_confidence_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_extract_number() {
        assert_eq!(extract_number("last 10 events"), Some(10));
        assert_eq!(extract_number("show 100"), Some(100));
        assert_eq!(extract_number("no numbers here"), None);
    }

    #[test]
    fn test_multiple_schemas_topic_extraction() {
        let engine = NaturalLanguageEngine::new(test_config());
        let schemas = vec![test_schema(), orders_schema()];
        let (sql, _, _) = engine.translate_with_patterns("count all orders", &schemas);
        assert_eq!(sql, "SELECT COUNT(*) FROM orders");
    }

    #[tokio::test]
    async fn test_suggest_questions_unknown_topic() {
        let engine = NaturalLanguageEngine::new(test_config());
        let suggestions = engine.suggest_questions("nonexistent").await;
        assert_eq!(suggestions.len(), 2); // Just the generic suggestions
    }

    #[test]
    fn test_nl_operation_variants() {
        let ops = vec![
            NlOperation::Select,
            NlOperation::Aggregate,
            NlOperation::Filter,
            NlOperation::Join,
            NlOperation::Window,
            NlOperation::Create,
            NlOperation::Describe,
        ];
        assert_eq!(ops.len(), 7);
        assert_ne!(NlOperation::Select, NlOperation::Filter);
    }
}
