use colored::Colorize;
use std::path::Path;

use streamline::cli_utils::fuzzy::SuggestionContext;
use streamline::StreamlineError;

/// Structured error information for user-friendly error display
struct ErrorInfo {
    /// What happened
    what: String,
    /// Why it happened (optional)
    why: Option<String>,
    /// How to fix it
    fix: Vec<String>,
    /// Documentation link (optional)
    docs: Option<&'static str>,
}

const DOCS_BASE: &str = "https://github.com/josedab/streamline";

/// Print helpful, structured error information
pub(crate) fn print_error_hint(e: &StreamlineError, data_dir: &Path) {
    // Load suggestion context for fuzzy matching
    let suggestions = SuggestionContext::load_from_data_dir(data_dir);

    let info: Option<ErrorInfo> = match e {
        StreamlineError::TopicNotFound(name) => {
            let mut fix = vec![
                "streamline-cli topics list".into(),
                format!("streamline-cli topics create {}", name),
            ];

            // Suggest similar topics if available
            if let Some(suggested) = suggestions.suggest_topic(name) {
                fix.push(suggested);
            }

            Some(ErrorInfo {
                what: format!("Topic '{}' not found", name),
                why: Some("The topic does not exist on this server.".into()),
                fix,
                docs: Some("/blob/main/docs/CLI.md#topic-management"),
            })
        }

        StreamlineError::PartitionNotFound(topic, partition) => Some(ErrorInfo {
            what: format!("Partition {} not found in topic '{}'", partition, topic),
            why: Some("The topic has fewer partitions than requested.".into()),
            fix: vec![
                format!("Check topic partitions: streamline-cli topics describe {}", topic),
                "Use a partition number within the topic's partition count".into(),
            ],
            docs: Some("/blob/main/docs/CLI.md#topic-management"),
        }),

        StreamlineError::TopicAlreadyExists(name) => Some(ErrorInfo {
            what: format!("Topic '{}' already exists", name),
            why: Some("Topic names must be unique.".into()),
            fix: vec![
                "Use a different topic name".into(),
                format!("Delete existing topic: streamline-cli topics delete {}", name),
            ],
            docs: Some("/blob/main/docs/CLI.md#topic-management"),
        }),

        StreamlineError::InvalidTopicName(name) => Some(ErrorInfo {
            what: format!("Invalid topic name '{}'", name),
            why: Some("Topic names must be 1-249 characters and contain only letters, numbers, dots, underscores, and hyphens.".into()),
            fix: vec![
                "Use a valid topic name (e.g., 'events', 'user-events', 'orders.v1')".into(),
            ],
            docs: Some("/blob/main/docs/CLI.md#topic-management"),
        }),

        StreamlineError::Storage(ref msg) if msg.contains("not found") => Some(ErrorInfo {
            what: "Resource not found in storage".into(),
            why: Some("The requested data does not exist on disk.".into()),
            fix: vec!["streamline-cli topics list".into()],
            docs: None,
        }),

        StreamlineError::Storage(ref msg) if msg.contains("permission") || msg.contains("denied") => {
            Some(ErrorInfo {
                what: "Storage permission denied".into(),
                why: Some("The process lacks permissions to access the data directory.".into()),
                fix: vec![
                    "Check file permissions on the data directory".into(),
                    "Ensure the data directory exists and is writable".into(),
                ],
                docs: None,
            })
        }

        StreamlineError::Storage(ref msg) if msg.contains("disk") || msg.contains("space") => {
            Some(ErrorInfo {
                what: "Insufficient disk space".into(),
                why: Some("The storage volume is full or nearly full.".into()),
                fix: vec![
                    "Free up disk space or change --data-dir".into(),
                    "Enable log compaction or reduce retention".into(),
                ],
                docs: Some("/blob/main/README.md#topic-configuration"),
            })
        }

        StreamlineError::Storage(_) | StreamlineError::StorageDomain(_) => Some(ErrorInfo {
            what: "Storage error".into(),
            why: Some("The data directory may be missing or corrupted.".into()),
            fix: vec![
                format!("Check data directory: ls -la {}", data_dir.display()),
                "Ensure the server has been started at least once".into(),
            ],
            docs: Some("/blob/main/docs/TROUBLESHOOTING.md"),
        }),

        StreamlineError::Config(ref msg) if msg.contains("Invalid") => Some(ErrorInfo {
            what: "Invalid configuration".into(),
            why: Some(format!("Configuration error: {}", msg)),
            fix: vec![
                "streamline --help".into(),
                "Verify configuration file syntax".into(),
            ],
            docs: Some("/blob/main/README.md#configuration"),
        }),

        StreamlineError::Config(_) | StreamlineError::ConfigDomain(_) => Some(ErrorInfo {
            what: "Configuration error".into(),
            why: Some("CLI options or configuration file are invalid.".into()),
            fix: vec![
                "Check command syntax with --help".into(),
                "Verify configuration file format".into(),
            ],
            docs: Some("/blob/main/docs/CONFIGURATION.md"),
        }),

        StreamlineError::AuthenticationFailed(_) | StreamlineError::AuthorizationFailed(_) => {
            Some(ErrorInfo {
                what: "Authentication/Authorization error".into(),
                why: Some("Credentials are missing, invalid, or insufficient.".into()),
                fix: vec![
                    "Check username/password or SASL configuration".into(),
                    "Ensure user has required permissions".into(),
                ],
                docs: Some("/blob/main/docs/AUTHENTICATION.md"),
            })
        }

        StreamlineError::InvalidCredentials => Some(ErrorInfo {
            what: "Invalid credentials".into(),
            why: Some("Username or password is incorrect.".into()),
            fix: vec![
                "Verify credentials".into(),
                "Reset password if needed".into(),
            ],
            docs: Some("/blob/main/docs/AUTHENTICATION.md"),
        }),

        StreamlineError::MessageTooLarge(size, max) => Some(ErrorInfo {
            what: "Message too large".into(),
            why: Some(format!("Message size {} exceeds maximum {}", size, max)),
            fix: vec![
                "Reduce message size".into(),
                "Increase max message size in config".into(),
            ],
            docs: Some("/blob/main/docs/CONFIGURATION.md#storage"),
        }),

        StreamlineError::ConnectionLimitExceeded => Some(ErrorInfo {
            what: "Connection limit exceeded".into(),
            why: Some("Too many concurrent connections.".into()),
            fix: vec![
                "Close unused connections".into(),
                "Increase connection limits in config".into(),
            ],
            docs: Some("/blob/main/docs/CONFIGURATION.md#limits"),
        }),

        StreamlineError::ShuttingDown => Some(ErrorInfo {
            what: "Server is shutting down".into(),
            why: Some("The server is in the process of stopping.".into()),
            fix: vec!["Wait for shutdown to complete or restart".into()],
            docs: None,
        }),

        StreamlineError::Protocol(_) | StreamlineError::ProtocolDomain(_) => Some(ErrorInfo {
            what: "Protocol error".into(),
            why: Some("The request was malformed or unsupported.".into()),
            fix: vec![
                "Check client configuration".into(),
                "Ensure client is Kafka-compatible".into(),
            ],
            docs: Some("/blob/main/docs/PROTOCOL_COMPLIANCE_AUDIT.md"),
        }),

        StreamlineError::Server(_) | StreamlineError::ServerDomain(_) => Some(ErrorInfo {
            what: "Server error".into(),
            why: Some("The server encountered an internal error.".into()),
            fix: vec![
                "Check server logs for details".into(),
                "Restart the server if the error persists".into(),
            ],
            docs: Some("/blob/main/docs/TROUBLESHOOTING.md"),
        }),


        StreamlineError::Io(io_err) if io_err.kind() == std::io::ErrorKind::ConnectionRefused => {
            Some(ErrorInfo {
                what: "Connection refused".into(),
                why: Some("The server is not running or not accepting connections.".into()),
                fix: vec![
                    "Ensure the Streamline server is running".into(),
                    "Check the server address and port".into(),
                    "Start with: streamline --playground".into(),
                ],
                docs: Some("/blob/main/README.md#quick-start"),
            })
        }

        StreamlineError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            Some(ErrorInfo {
                what: "File or directory not found".into(),
                why: Some("The specified path does not exist.".into()),
                fix: vec![
                    "Check that the path is correct".into(),
                    "Ensure the data directory exists".into(),
                ],
                docs: None,
            })
        }

        StreamlineError::Io(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
            Some(ErrorInfo {
                what: "Permission denied".into(),
                why: Some("The process lacks file system permissions.".into()),
                fix: vec![
                    "Check file and directory permissions".into(),
                    "Try running with appropriate permissions".into(),
                ],
                docs: None,
            })
        }

        StreamlineError::Cluster(_) | StreamlineError::ClusterDomain(_) => Some(ErrorInfo {
            what: "Cluster error".into(),
            why: Some("Cluster coordination failed.".into()),
            fix: vec![
                "streamline-cli cluster validate".into(),
                "Ensure all cluster nodes are reachable".into(),
            ],
            docs: Some("/blob/main/README.md#clustering-configuration"),
        }),

        StreamlineError::NotLeader(topic, partition) => Some(ErrorInfo {
            what: format!(
                "Not leader for partition {} of topic '{}'",
                partition, topic
            ),
            why: Some("The request was sent to a follower node.".into()),
            fix: vec!["This is usually transient - retry the operation".into()],
            docs: None,
        }),

        StreamlineError::RateLimitExceeded => Some(ErrorInfo {
            what: "Rate limit exceeded".into(),
            why: Some("Too many requests in a short time period.".into()),
            fix: vec![
                "Reduce request rate or wait before retrying".into(),
                "Increase --quota-producer-byte-rate on server".into(),
            ],
            docs: Some("/blob/main/README.md#client-quotas-configuration"),
        }),

        StreamlineError::RequestTimeout => Some(ErrorInfo {
            what: "Request timed out".into(),
            why: Some("The server did not respond in time.".into()),
            fix: vec![
                "The server may be overloaded - try again later".into(),
                "Check network connectivity to the server".into(),
            ],
            docs: None,
        }),

        StreamlineError::CorruptedData(_) => Some(ErrorInfo {
            what: "Data corruption detected".into(),
            why: Some("Segment data failed integrity checks.".into()),
            fix: vec![
                "Check storage integrity".into(),
                "Recover from backup if available".into(),
            ],
            docs: None,
        }),

        StreamlineError::Replication(_) => Some(ErrorInfo {
            what: "Replication error".into(),
            why: Some("Data replication between nodes failed.".into()),
            fix: vec![
                "Check that all replicas are online".into(),
                "Verify network connectivity between brokers".into(),
            ],
            docs: Some("/blob/main/README.md#clustering-configuration"),
        }),

        StreamlineError::InvalidOffset(_) => Some(ErrorInfo {
            what: "Invalid offset".into(),
            why: Some("The specified offset is out of range.".into()),
            fix: vec![
                "Use --from-beginning to start from the earliest offset".into(),
                "Check available offsets with topics describe".into(),
            ],
            docs: None,
        }),

        _ => None,
    };

    if let Some(info) = info {
        // Print structured error info
        eprintln!();
        eprintln!("  {} {}", "What:".cyan().bold(), info.what);

        if let Some(why) = &info.why {
            eprintln!("  {} {}", "Why:".cyan().bold(), why);
        }

        if !info.fix.is_empty() {
            eprintln!("  {}", "Fix:".cyan().bold());
            for fix in &info.fix {
                eprintln!("    {} {}", "→".cyan(), fix);
            }
        }

        if let Some(docs) = info.docs {
            eprintln!("  {} {}{}", "Docs:".cyan().bold(), DOCS_BASE, docs);
        }
        eprintln!();
    }
}
