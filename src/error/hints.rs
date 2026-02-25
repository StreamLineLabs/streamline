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

