//! Error hints and context for actionable error messages

use super::{StreamlineError, KafkaErrorCode};

/// Context for errors that can include available resources
#[derive(Debug, Clone, Default)]
pub struct ErrorContext {
    /// Available topics (for topic-related errors)
    pub available_topics: Option<Vec<String>>,
    /// Available partitions (for partition-related errors)
    pub partition_count: Option<i32>,
    /// Available consumer groups
    pub available_groups: Option<Vec<String>>,
    /// Server version info
    pub server_version: Option<String>,
    /// Documentation base URL
    pub docs_base_url: Option<String>,
}

impl ErrorContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.available_topics = Some(topics);
        self
    }

    pub fn with_partition_count(mut self, count: i32) -> Self {
        self.partition_count = Some(count);
        self
    }

    pub fn with_groups(mut self, groups: Vec<String>) -> Self {
        self.available_groups = Some(groups);
        self
    }

    pub fn with_docs_url(mut self, url: impl Into<String>) -> Self {
        self.docs_base_url = Some(url.into());
        self
    }
}

/// Extension trait for adding hints to errors
pub trait ErrorHint {
    /// Get a helpful hint for resolving this error
    fn hint(&self) -> Option<String>;

    /// Get a hint with additional context about available resources
    fn hint_with_context(&self, ctx: &ErrorContext) -> Option<String>;

    /// Get CLI command(s) to fix this error
    fn suggest_fix(&self) -> Option<String>;

    /// Get CLI command(s) to fix with context
    fn suggest_fix_with_context(&self, ctx: &ErrorContext) -> Option<String>;

    /// Get documentation URL for this error
    fn docs_url(&self) -> Option<String>;

    /// Format the error with hint for display
    fn with_hint(&self) -> String;

    /// Format error with full context (hint, fix, docs)
    fn format_with_context(&self, ctx: &ErrorContext) -> String;
}

impl ErrorHint for StreamlineError {
    /// Returns an actionable hint for resolving this error
    fn hint(&self) -> Option<String> {
        match self {
            StreamlineError::TopicNotFound(topic) => Some(format!(
                "Topic '{}' does not exist. List available topics with: `streamline-cli topics list` or create it with: `streamline-cli topics create {} --partitions 3`",
                topic, topic
            )),
            StreamlineError::PartitionNotFound(topic, partition) => Some(format!(
                "Partition {} does not exist for topic '{}' (topic may have fewer than {} partitions). Check available partitions with: `streamline-cli topics describe {}`",
                partition, topic, partition + 1, topic
            )),
            StreamlineError::TopicAlreadyExists(topic) => Some(format!(
                "Topic '{}' already exists. Inspect it with: `streamline-cli topics describe {}` or delete with: `streamline-cli topics delete {}`",
                topic, topic, topic
            )),
            StreamlineError::InvalidTopicName(name) => Some(format!(
                "Topic name '{}' is invalid. Names must be 1-255 characters using alphanumeric, '.', '_', or '-'. Validate with: `streamline-cli topics validate {}`",
                name, name.replace('/', "_")
            )),
            StreamlineError::InvalidPartitionCount(msg) => Some(format!(
                "{}. Partition count must be between 1 and 10,000. Use: `streamline-cli topics create <name> --partitions <N>`",
                msg
            )),
            StreamlineError::AuthenticationFailed(_) => Some(
                "Authentication failed. Check credentials in `~/.streamline/config.toml` or verify with: `streamline-cli doctor --check auth`".into()
            ),
            StreamlineError::AuthorizationFailed(_) => Some(
                "Permission denied. Review ACLs with: `streamline-cli acls list` or contact your admin to grant access".into()
            ),
            StreamlineError::InvalidCredentials => Some(
                "Invalid credentials. Reset with: `streamline-cli auth configure` or check `~/.streamline/config.toml`".into()
            ),
            StreamlineError::MessageTooLarge(size, max) => Some(format!(
                "Message size {} exceeds limit of {}. Reduce message size or increase `max.message.bytes` in server config: `streamline-cli config set max.message.bytes {}`",
                size, max, size
            )),
            StreamlineError::InvalidOffset(offset) => Some(format!(
                "Offset {} is out of range. Reset consumer position with: `streamline-cli consume <topic> --from-beginning` or check valid offsets with: `streamline-cli topics offsets <topic>`",
                offset
            )),
            StreamlineError::RequestTimeout => Some(
                "Request timed out. Check server status with: `streamline-cli doctor --check connectivity` or increase timeout: `--timeout 60000`".into()
            ),
            StreamlineError::RateLimitExceeded => Some(
                "Rate limit exceeded. Check current quotas with: `streamline-cli quotas describe` or increase limits in server configuration".into()
            ),
            StreamlineError::ConnectionLimitExceeded => Some(
                "Too many connections. Check active connections with: `streamline-cli connections list` or increase `max_connections` in server config".into()
            ),
            StreamlineError::Storage(msg) if msg.contains("disk") || msg.contains("space") => Some(
                "Disk space may be low. Check usage with: `streamline-cli storage status` and free space with: `streamline-cli storage compact --topic <topic>`".into()
            ),
            StreamlineError::Storage(_) | StreamlineError::StorageDomain(_) => Some(
                "Storage error. Run diagnostics with: `streamline-cli doctor --deep` and check permissions on the data directory".into()
            ),
            StreamlineError::Config(_) | StreamlineError::ConfigDomain(_) => Some(
                "Configuration error. Validate config with: `streamline-cli config validate` or generate a reference config: `streamline --generate-config > streamline.toml`".into()
            ),


            StreamlineError::Protocol(_) | StreamlineError::ProtocolDomain(_) => Some(
                "Protocol error. Check version compatibility with: `streamline-cli info` and ensure your client SDK is up to date".into()
            ),
            StreamlineError::Cluster(_) | StreamlineError::ClusterDomain(_) => Some(
                "Cluster error. Check cluster health with: `streamline-cli cluster status` and verify node connectivity: `streamline-cli doctor --check cluster`".into()
            ),
            StreamlineError::NotLeader(topic, partition) => Some(format!(
                "This node is not the leader for {}/{}. Refresh metadata with: `streamline-cli metadata refresh` — clients should auto-retry",
                topic, partition
            )),
            StreamlineError::Replication(_) => Some(
                "Replication error. Check replica status with: `streamline-cli cluster replicas` and verify network between nodes: `streamline-cli doctor --check cluster`".into()
            ),
            StreamlineError::CorruptedData(_) => Some(
                "Data corruption detected. Run full diagnostics: `streamline-cli doctor --deep` and check affected segments: `streamline-cli storage verify`".into()
            ),
            StreamlineError::Network(_) => Some(
                "Network error. Verify connectivity with: `streamline-cli doctor --check connectivity` and check firewall rules for port 9092".into()
            ),
            StreamlineError::Timeout(_) => Some(
                "Operation timed out. Check server load with: `streamline-cli metrics` and consider increasing timeout: `--timeout 60000`".into()
            ),
            StreamlineError::ShuttingDown => Some(
                "Server is shutting down. Monitor restart with: `streamline-cli doctor --watch` or connect to another node: `streamline-cli cluster nodes`".into()
            ),
            StreamlineError::ResourceExhausted(_) => Some(
                "System resources exhausted. Check resource usage: `streamline-cli metrics --system` and review limits in server configuration".into()
            ),
            StreamlineError::Analytics(msg) => Some(format!(
                "Analytics query failed: {}. Verify SQL syntax with: `streamline-cli query --validate <sql>` or check the DuckDB documentation for supported functions",
                msg
            )),
            StreamlineError::Sink(_) => Some(
                "Sink operation failed. Check sink configuration with: `streamline-cli connectors status` and verify the destination is reachable".into()
            ),
            StreamlineError::Query(_) => Some(
                "Query execution failed. Check SQL syntax with: `streamline-cli query --validate <sql>` and ensure the target topic exists".into()
            ),
            StreamlineError::Parse(_) => Some(
                "Failed to parse input. Verify the input format matches the expected schema. Use: `streamline-cli schema describe <subject>` to check expected format".into()
            ),
            StreamlineError::Wasm(_) => Some(
                "WASM transform error. Validate the transform with: `streamline-cli transforms validate <path>` or check transform logs: `streamline-cli transforms logs <name>`".into()
            ),
            StreamlineError::InvalidData(_) => Some(
                "Invalid data encountered. Check message format and encoding. Use: `streamline-cli consume <topic> --format raw` to inspect raw messages".into()
            ),
            StreamlineError::AI(_) => Some(
                "AI/ML pipeline error. Check model configuration with: `streamline-cli ai status` and verify model endpoint connectivity".into()
            ),
            StreamlineError::Crdt(_) => Some(
                "Conflict resolution error in geo-replicated data. Check replication status: `streamline-cli cluster replicas` and review conflict resolution policy".into()
            ),
            StreamlineError::Tenant(_) => Some(
                "Tenant operation failed. Verify tenant exists with: `streamline-cli tenants list` and check quota limits: `streamline-cli tenants describe <id>`".into()
            ),
            StreamlineError::Validation(_) => Some(
                "Validation failed. Check input against the expected schema or constraints. Use: `streamline-cli schema describe <subject>` for schema details".into()
            ),
            StreamlineError::Internal(_) => Some(
                "Internal server error. Check server logs: `streamline-cli logs --tail 50` and report persistent issues at https://github.com/streamlinelabs/streamline/issues".into()
            ),
            StreamlineError::Mcp(_) => Some(
                "MCP (Model Context Protocol) error. Check MCP endpoint configuration and verify the MCP server is running".into()
            ),
            StreamlineError::Cdc(_) => Some(
                "CDC (Change Data Capture) error. Check connector status: `streamline-cli connectors status` and verify source database connectivity and permissions".into()
            ),
            StreamlineError::Connector(_) => Some(
                "Connector error. Check connector status: `streamline-cli connectors list` and review connector logs: `streamline-cli connectors logs <name>`".into()
            ),
            StreamlineError::Pipeline(_) => Some(
                "Pipeline processing error. Check pipeline status: `streamline-cli pipelines status` and verify all stages are configured correctly".into()
            ),
            StreamlineError::Namespace(_) => Some(
                "Namespace error. List available namespaces: `streamline-cli namespaces list` or create one: `streamline-cli namespaces create <name>`".into()
            ),
            StreamlineError::ReplicationConflict(_) => Some(
                "Replication conflict detected between regions. Check conflict resolution policy: `streamline-cli cluster geo-status` and review conflict log".into()
            ),
            StreamlineError::QuotaExceeded(_) => Some(
                "Quota exceeded for this tenant or client. Check current usage: `streamline-cli quotas describe` and request a quota increase if needed".into()
            ),
            StreamlineError::ContractViolation(_) => Some(
                "API contract violation. Ensure your request matches the expected format. Check API docs: `streamline-cli docs open`".into()
            ),
            StreamlineError::Debugger(_) => Some(
                "Debugger error. Restart the debug session: `streamline-cli debug --reset` or check debug port availability".into()
            ),
            StreamlineError::Marketplace(_) => Some(
                "Marketplace operation failed. Check marketplace connectivity: `streamline-cli marketplace status` and verify your authentication token".into()
            ),
            StreamlineError::Gateway(_) => Some(
                "Gateway error. Check gateway status: `streamline-cli gateway status` and verify the upstream service is healthy".into()
            ),
            StreamlineError::Rebalance(_) => Some(
                "Consumer group rebalance error. This is usually transient — consumers will rejoin automatically. Monitor with: `streamline-cli groups describe <group>`".into()
            ),
            StreamlineError::Playground(_) => Some(
                "Playground error. Restart the playground: `streamline --playground` or reset state: `streamline-cli playground reset`".into()
            ),
            StreamlineError::Lineage(_) => Some(
                "Data lineage tracking error. Check lineage configuration: `streamline-cli lineage status` and verify lineage storage is accessible".into()
            ),
            StreamlineError::Ffi(_) => Some(
                "Foreign function interface error. Check that the native library is compatible with this platform and architecture".into()
            ),
            StreamlineError::InvalidClientId(_) => Some(
                "Client ID is invalid. Use alphanumeric characters, dots, underscores, and hyphens only (1-255 chars)".into()
            ),
            StreamlineError::Io(_) => Some(
                "I/O error. Check file system permissions and disk space: `streamline-cli doctor --check storage`".into()
            ),
            StreamlineError::Serialization(_) => Some(
                "Serialization error. Check that the message format matches the expected schema. Use: `streamline-cli schema describe <subject>` for details".into()
            ),
            StreamlineError::Server(_) | StreamlineError::ServerDomain(_) => Some(
                "Server error. Check server logs for details: `streamline-cli logs --tail 50` and restart if the error persists".into()
            ),
            _ => None,
        }
    }

    /// Returns a hint with additional context about available resources
    fn hint_with_context(&self, ctx: &ErrorContext) -> Option<String> {
        match self {
            StreamlineError::TopicNotFound(topic) => {
                let base = format!("Topic '{}' not found.", topic);
                if let Some(ref topics) = ctx.available_topics {
                    if topics.is_empty() {
                        Some(format!("{} No topics exist yet.", base))
                    } else {
                        let display_topics: Vec<_> = topics.iter().take(5).collect();
                        let suffix = if topics.len() > 5 {
                            format!(" (and {} more)", topics.len() - 5)
                        } else {
                            String::new()
                        };
                        Some(format!(
                            "{} Available topics: {}{}",
                            base,
                            display_topics
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            suffix
                        ))
                    }
                } else {
                    self.hint()
                }
            }
            StreamlineError::PartitionNotFound(topic, partition) => {
                if let Some(count) = ctx.partition_count {
                    Some(format!(
                        "Partition {} does not exist. Topic '{}' has {} partition{}. Valid range: 0-{}",
                        partition, topic, count,
                        if count == 1 { "" } else { "s" },
                        count - 1
                    ))
                } else {
                    self.hint()
                }
            }
            _ => self.hint(),
        }
    }

    /// Returns CLI command(s) to fix this error
    fn suggest_fix(&self) -> Option<String> {
        match self {
            StreamlineError::TopicNotFound(topic) => Some(format!(
                "streamline-cli topics create {} --partitions 3",
                topic
            )),
            StreamlineError::TopicAlreadyExists(topic) => {
                Some(format!("streamline-cli topics delete {}", topic))
            }
            StreamlineError::InvalidOffset(_) => {
                Some("streamline-cli consume <topic> --from-beginning".into())
            }
            StreamlineError::AuthenticationFailed(_) | StreamlineError::InvalidCredentials => {
                Some("streamline-cli doctor --check auth".into())
            }
            StreamlineError::Storage(_) | StreamlineError::StorageDomain(_)
            | StreamlineError::CorruptedData(_) => {
                Some("streamline-cli doctor --deep".into())
            }
            StreamlineError::RequestTimeout | StreamlineError::Network(_) => {
                Some("streamline-cli doctor".into())
            }
            StreamlineError::Cluster(_) | StreamlineError::ClusterDomain(_) => {
                Some("streamline-cli cluster status".into())
            }
            StreamlineError::Config(_) | StreamlineError::ConfigDomain(_) => {
                Some("streamline --generate-config > streamline.toml".into())
            }
            StreamlineError::RateLimitExceeded => {
                Some("# Increase quotas in config or reduce client request rate".into())
            }
            StreamlineError::Analytics(_) | StreamlineError::Query(_) => {
                Some("streamline-cli query --validate '<your-sql>'".into())
            }
            StreamlineError::Sink(_) | StreamlineError::Connector(_) | StreamlineError::Cdc(_) => {
                Some("streamline-cli connectors status".into())
            }
            StreamlineError::Pipeline(_) => {
                Some("streamline-cli pipelines status".into())
            }
            StreamlineError::Wasm(_) => {
                Some("streamline-cli transforms validate <path-to-wasm>".into())
            }
            StreamlineError::Tenant(_) | StreamlineError::QuotaExceeded(_) => {
                Some("streamline-cli quotas describe".into())
            }
            StreamlineError::Namespace(_) => {
                Some("streamline-cli namespaces list".into())
            }
            StreamlineError::Rebalance(_) => {
                Some("streamline-cli groups describe <group-id>".into())
            }
            StreamlineError::Marketplace(_) => {
                Some("streamline-cli marketplace status".into())
            }
            StreamlineError::Internal(_) | StreamlineError::Server(_) | StreamlineError::ServerDomain(_) => {
                Some("streamline-cli logs --tail 50".into())
            }
            StreamlineError::Playground(_) => {
                Some("streamline --playground".into())
            }
            _ => None,
        }
    }

    /// Returns CLI command(s) to fix with context
    fn suggest_fix_with_context(&self, ctx: &ErrorContext) -> Option<String> {
        match self {
            StreamlineError::TopicNotFound(topic) => {
                // Suggest similar topic names if available
                if let Some(ref topics) = ctx.available_topics {
                    let similar: Vec<_> = topics
                        .iter()
                        .filter(|t| {
                            t.contains(topic)
                                || topic.contains(t.as_str())
                                || levenshtein_distance(t, topic) <= 3
                        })
                        .take(3)
                        .collect();

                    if !similar.is_empty() {
                        return Some(format!(
                            "# Did you mean one of these?\n{}",
                            similar
                                .iter()
                                .map(|t| format!("streamline-cli topics describe {}", t))
                                .collect::<Vec<_>>()
                                .join("\n")
                        ));
                    }
                }
                self.suggest_fix()
            }
            _ => self.suggest_fix(),
        }
    }

    /// Returns documentation URL for this error
    fn docs_url(&self) -> Option<String> {
        let base = "https://streamline.dev/docs";
        match self {
            StreamlineError::TopicNotFound(_)
            | StreamlineError::TopicAlreadyExists(_)
            | StreamlineError::InvalidTopicName(_)
            | StreamlineError::InvalidPartitionCount(_) => Some(format!("{}/reference/topics", base)),
            StreamlineError::PartitionNotFound(_, _) => {
                Some(format!("{}/concepts/partitions", base))
            }
            StreamlineError::AuthenticationFailed(_) | StreamlineError::InvalidCredentials => {
                Some(format!("{}/security/authentication", base))
            }
            StreamlineError::AuthorizationFailed(_) => {
                Some(format!("{}/security/authorization", base))
            }
            StreamlineError::Storage(_) | StreamlineError::StorageDomain(_)
            | StreamlineError::CorruptedData(_) => {
                Some(format!("{}/operations/storage", base))
            }
            StreamlineError::Config(_) | StreamlineError::ConfigDomain(_) => {
                Some(format!("{}/configuration", base))
            }
            StreamlineError::Cluster(_)
            | StreamlineError::ClusterDomain(_)
            | StreamlineError::NotLeader(_, _)
            | StreamlineError::Replication(_) => Some(format!("{}/clustering", base)),
            StreamlineError::MessageTooLarge(_, _) => Some(format!("{}/reference/limits", base)),
            StreamlineError::RateLimitExceeded => Some(format!("{}/operations/quotas", base)),
            StreamlineError::Protocol(_) | StreamlineError::ProtocolDomain(_) => {
                Some(format!("{}/reference/protocol", base))
            }
            StreamlineError::Analytics(_) | StreamlineError::Query(_) => {
                Some(format!("{}/features/analytics", base))
            }
            StreamlineError::Sink(_) | StreamlineError::Connector(_) | StreamlineError::Cdc(_) => {
                Some(format!("{}/features/cdc", base))
            }
            StreamlineError::Pipeline(_) | StreamlineError::Wasm(_) => {
                Some(format!("{}/features/transforms", base))
            }
            StreamlineError::Tenant(_) | StreamlineError::Namespace(_)
            | StreamlineError::QuotaExceeded(_) => {
                Some(format!("{}/operations/multi-tenancy", base))
            }
            StreamlineError::ReplicationConflict(_) | StreamlineError::Crdt(_) => {
                Some(format!("{}/features/geo-replication", base))
            }
            StreamlineError::Rebalance(_) => {
                Some(format!("{}/concepts/consumer-groups", base))
            }
            StreamlineError::Marketplace(_) => {
                Some(format!("{}/features/marketplace", base))
            }
            StreamlineError::Gateway(_) => {
                Some(format!("{}/reference/grpc-api", base))
            }
            StreamlineError::Playground(_) => {
                Some(format!("{}/getting-started/playground", base))
            }
            StreamlineError::Lineage(_) => {
                Some(format!("{}/features/lineage", base))
            }
            StreamlineError::AI(_) => {
                Some(format!("{}/features/ai", base))
            }
            StreamlineError::Validation(_) | StreamlineError::InvalidData(_)
            | StreamlineError::Serialization(_) | StreamlineError::Parse(_) => {
                Some(format!("{}/reference/schema-registry", base))
            }
            StreamlineError::Network(_) | StreamlineError::Io(_) => {
                Some(format!("{}/operations/troubleshooting", base))
            }
            StreamlineError::Internal(_) | StreamlineError::Server(_) | StreamlineError::ServerDomain(_) => {
                Some(format!("{}/operations/troubleshooting", base))
            }
            _ => None,
        }
    }

    /// Format error with hint for user-friendly display
    fn with_hint(&self) -> String {
        let error_msg = self.to_string();
        match self.hint() {
            Some(hint) => format!("{}\n  hint: {}", error_msg, hint),
            None => error_msg,
        }
    }

    /// Format error with full context (hint, fix, docs)
    fn format_with_context(&self, ctx: &ErrorContext) -> String {
        let mut output = format!("Error: {}", self);

        if let Some(hint) = self.hint_with_context(ctx) {
            output.push_str(&format!("\n\n  Hint: {}", hint));
        }

        if let Some(fix) = self.suggest_fix_with_context(ctx) {
            output.push_str(&format!("\n\n  Fix:\n    {}", fix.replace('\n', "\n    ")));
        }

        if let Some(url) = self.docs_url() {
            output.push_str(&format!("\n\n  Docs: {}", url));
        }

        output
    }
}

/// Simple Levenshtein distance for fuzzy matching topic names
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_len = a.chars().count();
    let b_len = b.chars().count();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev_row: Vec<usize> = (0..=b_len).collect();
    let mut curr_row = vec![0; b_len + 1];

    for (i, a_char) in a.chars().enumerate() {
        curr_row[0] = i + 1;
        for (j, b_char) in b.chars().enumerate() {
            let cost = if a_char == b_char { 0 } else { 1 };
            curr_row[j + 1] = (prev_row[j + 1] + 1)
                .min(curr_row[j] + 1)
                .min(prev_row[j] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[b_len]
}

