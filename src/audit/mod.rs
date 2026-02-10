//! Security audit logging for Streamline
//!
//! This module provides comprehensive audit logging for security-critical operations
//! including authentication attempts, authorization decisions, and administrative actions.

use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{debug, error, info};

/// Audit event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum AuditEvent {
    /// Successful authentication
    #[serde(rename = "AUTH_SUCCESS")]
    AuthSuccess {
        user: String,
        mechanism: String,
        client_ip: String,
    },

    /// Failed authentication attempt
    #[serde(rename = "AUTH_FAILURE")]
    AuthFailure {
        user: Option<String>,
        mechanism: String,
        client_ip: String,
        reason: String,
    },

    /// Session logout/termination
    #[serde(rename = "AUTH_LOGOUT")]
    AuthLogout {
        user: String,
        client_ip: String,
        duration_secs: u64,
    },

    /// Authorization allowed
    #[serde(rename = "ACL_ALLOW")]
    AclAllow {
        user: String,
        resource_type: String,
        resource_name: String,
        operation: String,
        client_ip: String,
    },

    /// Authorization denied
    #[serde(rename = "ACL_DENY")]
    AclDeny {
        user: String,
        resource_type: String,
        resource_name: String,
        operation: String,
        client_ip: String,
        reason: String,
    },

    /// Topic created
    #[serde(rename = "TOPIC_CREATE")]
    TopicCreate {
        user: Option<String>,
        topic: String,
        partitions: i32,
        client_ip: String,
    },

    /// Topic deleted
    #[serde(rename = "TOPIC_DELETE")]
    TopicDelete {
        user: Option<String>,
        topic: String,
        client_ip: String,
    },

    /// Node joined cluster
    #[serde(rename = "NODE_JOIN")]
    NodeJoin { node_id: u64, address: String },

    /// Node left cluster
    #[serde(rename = "NODE_LEAVE")]
    NodeLeave { node_id: u64, reason: String },

    /// Partition leadership changed
    #[serde(rename = "LEADER_CHANGE")]
    LeaderChange {
        topic: String,
        partition: i32,
        old_leader: Option<u64>,
        new_leader: u64,
    },

    /// Configuration changed
    #[serde(rename = "CONFIG_CHANGE")]
    ConfigChange {
        user: Option<String>,
        setting: String,
        old_value: Option<String>,
        new_value: String,
    },

    /// Connection attempt (for high-security environments)
    #[serde(rename = "CONNECTION")]
    Connection {
        client_ip: String,
        action: String, // "connect" or "disconnect"
        tls_enabled: bool,
    },
}

/// Audit log entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    /// Timestamp in RFC3339 format
    pub timestamp: String,

    /// Node ID that generated this event
    pub node_id: u64,

    /// Correlation ID for request tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// The audit event
    #[serde(flatten)]
    pub event: AuditEvent,
}

/// Audit log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,

    /// Path to audit log file
    pub log_path: Option<PathBuf>,

    /// Log format: "json" or "text"
    pub format: String,

    /// Events to log (empty = all events)
    pub events: Vec<String>,

    /// Also log to stdout
    pub log_to_stdout: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_path: None,
            format: "json".to_string(),
            events: vec![], // Empty means all events
            log_to_stdout: false,
        }
    }
}

/// Audit logger for recording security events
pub struct AuditLogger {
    config: AuditConfig,
    node_id: u64,
    file_writer: Option<Mutex<BufWriter<File>>>,
}

impl AuditLogger {
    /// Create a new audit logger
    pub fn new(config: AuditConfig, node_id: u64) -> std::io::Result<Self> {
        let file_writer = if config.enabled {
            if let Some(ref path) = config.log_path {
                // Create parent directories if needed
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let file = OpenOptions::new().create(true).append(true).open(path)?;

                info!(path = %path.display(), "Audit logging enabled");
                Some(Mutex::new(BufWriter::new(file)))
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            config,
            node_id,
            file_writer,
        })
    }

    /// Check if audit logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Log an audit event
    pub fn log(&self, event: AuditEvent) {
        self.log_with_correlation(event, None);
    }

    /// Log an audit event with a correlation ID
    pub fn log_with_correlation(&self, event: AuditEvent, correlation_id: Option<String>) {
        if !self.config.enabled {
            return;
        }

        // Check if this event type should be logged
        if !self.should_log_event(&event) {
            return;
        }

        let entry = AuditLogEntry {
            timestamp: Utc::now().to_rfc3339(),
            node_id: self.node_id,
            correlation_id,
            event,
        };

        // Format the log entry
        let formatted = match self.config.format.as_str() {
            "text" => self.format_text(&entry),
            _ => self.format_json(&entry),
        };

        // Write to file if configured
        if let Some(ref writer) = self.file_writer {
            let mut guard = writer.lock();
            if let Err(e) = writeln!(guard, "{}", formatted) {
                error!(error = %e, "Failed to write audit log");
            }
            let _ = guard.flush();
        }

        // Write to stdout if configured
        if self.config.log_to_stdout {
            println!("{}", formatted);
        }

        // Also log at debug level via tracing
        debug!(audit_event = %formatted, "Audit event recorded");
    }

    /// Check if a specific event type should be logged
    fn should_log_event(&self, event: &AuditEvent) -> bool {
        if self.config.events.is_empty() {
            return true; // Log all events
        }

        let event_type = match event {
            AuditEvent::AuthSuccess { .. } => "AUTH_SUCCESS",
            AuditEvent::AuthFailure { .. } => "AUTH_FAILURE",
            AuditEvent::AuthLogout { .. } => "AUTH_LOGOUT",
            AuditEvent::AclAllow { .. } => "ACL_ALLOW",
            AuditEvent::AclDeny { .. } => "ACL_DENY",
            AuditEvent::TopicCreate { .. } => "TOPIC_CREATE",
            AuditEvent::TopicDelete { .. } => "TOPIC_DELETE",
            AuditEvent::NodeJoin { .. } => "NODE_JOIN",
            AuditEvent::NodeLeave { .. } => "NODE_LEAVE",
            AuditEvent::LeaderChange { .. } => "LEADER_CHANGE",
            AuditEvent::ConfigChange { .. } => "CONFIG_CHANGE",
            AuditEvent::Connection { .. } => "CONNECTION",
        };

        self.config.events.iter().any(|e| e == event_type)
    }

    /// Format entry as JSON
    fn format_json(&self, entry: &AuditLogEntry) -> String {
        serde_json::to_string(entry).unwrap_or_else(|_| format!("{:?}", entry))
    }

    /// Format entry as text
    fn format_text(&self, entry: &AuditLogEntry) -> String {
        let event_str = match &entry.event {
            AuditEvent::AuthSuccess {
                user,
                mechanism,
                client_ip,
            } => {
                format!(
                    "AUTH_SUCCESS user={} mechanism={} client_ip={}",
                    user, mechanism, client_ip
                )
            }
            AuditEvent::AuthFailure {
                user,
                mechanism,
                client_ip,
                reason,
            } => {
                format!(
                    "AUTH_FAILURE user={:?} mechanism={} client_ip={} reason={}",
                    user, mechanism, client_ip, reason
                )
            }
            AuditEvent::AuthLogout {
                user,
                client_ip,
                duration_secs,
            } => {
                format!(
                    "AUTH_LOGOUT user={} client_ip={} duration={}s",
                    user, client_ip, duration_secs
                )
            }
            AuditEvent::AclAllow {
                user,
                resource_type,
                resource_name,
                operation,
                client_ip,
            } => {
                format!(
                    "ACL_ALLOW user={} resource={}:{} operation={} client_ip={}",
                    user, resource_type, resource_name, operation, client_ip
                )
            }
            AuditEvent::AclDeny {
                user,
                resource_type,
                resource_name,
                operation,
                client_ip,
                reason,
            } => {
                format!(
                    "ACL_DENY user={} resource={}:{} operation={} client_ip={} reason={}",
                    user, resource_type, resource_name, operation, client_ip, reason
                )
            }
            AuditEvent::TopicCreate {
                user,
                topic,
                partitions,
                client_ip,
            } => {
                format!(
                    "TOPIC_CREATE user={:?} topic={} partitions={} client_ip={}",
                    user, topic, partitions, client_ip
                )
            }
            AuditEvent::TopicDelete {
                user,
                topic,
                client_ip,
            } => {
                format!(
                    "TOPIC_DELETE user={:?} topic={} client_ip={}",
                    user, topic, client_ip
                )
            }
            AuditEvent::NodeJoin { node_id, address } => {
                format!("NODE_JOIN node_id={} address={}", node_id, address)
            }
            AuditEvent::NodeLeave { node_id, reason } => {
                format!("NODE_LEAVE node_id={} reason={}", node_id, reason)
            }
            AuditEvent::LeaderChange {
                topic,
                partition,
                old_leader,
                new_leader,
            } => {
                format!(
                    "LEADER_CHANGE topic={} partition={} old_leader={:?} new_leader={}",
                    topic, partition, old_leader, new_leader
                )
            }
            AuditEvent::ConfigChange {
                user,
                setting,
                old_value,
                new_value,
            } => {
                format!(
                    "CONFIG_CHANGE user={:?} setting={} old={:?} new={}",
                    user, setting, old_value, new_value
                )
            }
            AuditEvent::Connection {
                client_ip,
                action,
                tls_enabled,
            } => {
                format!(
                    "CONNECTION client_ip={} action={} tls={}",
                    client_ip, action, tls_enabled
                )
            }
        };

        format!(
            "{} [node={}] {}{}",
            entry.timestamp,
            entry.node_id,
            event_str,
            entry
                .correlation_id
                .as_ref()
                .map(|id| format!(" correlation_id={}", id))
                .unwrap_or_default()
        )
    }

    // Convenience methods for common events

    /// Log a successful authentication
    pub fn log_auth_success(&self, user: &str, mechanism: &str, client_ip: &SocketAddr) {
        self.log(AuditEvent::AuthSuccess {
            user: user.to_string(),
            mechanism: mechanism.to_string(),
            client_ip: client_ip.to_string(),
        });
    }

    /// Log a failed authentication attempt
    pub fn log_auth_failure(
        &self,
        user: Option<&str>,
        mechanism: &str,
        client_ip: &SocketAddr,
        reason: &str,
    ) {
        self.log(AuditEvent::AuthFailure {
            user: user.map(|s| s.to_string()),
            mechanism: mechanism.to_string(),
            client_ip: client_ip.to_string(),
            reason: reason.to_string(),
        });
    }

    /// Log an ACL allow decision
    pub fn log_acl_allow(
        &self,
        user: &str,
        resource_type: &str,
        resource_name: &str,
        operation: &str,
        client_ip: &SocketAddr,
    ) {
        self.log(AuditEvent::AclAllow {
            user: user.to_string(),
            resource_type: resource_type.to_string(),
            resource_name: resource_name.to_string(),
            operation: operation.to_string(),
            client_ip: client_ip.to_string(),
        });
    }

    /// Log an ACL deny decision
    pub fn log_acl_deny(
        &self,
        user: &str,
        resource_type: &str,
        resource_name: &str,
        operation: &str,
        client_ip: &SocketAddr,
        reason: &str,
    ) {
        self.log(AuditEvent::AclDeny {
            user: user.to_string(),
            resource_type: resource_type.to_string(),
            resource_name: resource_name.to_string(),
            operation: operation.to_string(),
            client_ip: client_ip.to_string(),
            reason: reason.to_string(),
        });
    }

    /// Log topic creation
    pub fn log_topic_create(
        &self,
        user: Option<&str>,
        topic: &str,
        partitions: i32,
        client_ip: &SocketAddr,
    ) {
        self.log(AuditEvent::TopicCreate {
            user: user.map(|s| s.to_string()),
            topic: topic.to_string(),
            partitions,
            client_ip: client_ip.to_string(),
        });
    }

    /// Log topic deletion
    pub fn log_topic_delete(&self, user: Option<&str>, topic: &str, client_ip: &SocketAddr) {
        self.log(AuditEvent::TopicDelete {
            user: user.map(|s| s.to_string()),
            topic: topic.to_string(),
            client_ip: client_ip.to_string(),
        });
    }

    /// Log a connection event (connect or disconnect)
    pub fn log_connection(&self, client_ip: &SocketAddr, action: &str, tls_enabled: bool) {
        self.log(AuditEvent::Connection {
            client_ip: client_ip.to_string(),
            action: action.to_string(),
            tls_enabled,
        });
    }

    /// Log a client connection
    pub fn log_connect(&self, client_ip: &SocketAddr, tls_enabled: bool) {
        self.log_connection(client_ip, "connect", tls_enabled);
    }

    /// Log a client disconnection
    pub fn log_disconnect(&self, client_ip: &SocketAddr, tls_enabled: bool) {
        self.log_connection(client_ip, "disconnect", tls_enabled);
    }
}

/// A no-op audit logger for when auditing is disabled
#[allow(dead_code)]
pub struct NoOpAuditLogger;

#[allow(dead_code)]
impl NoOpAuditLogger {
    pub fn log(&self, _event: AuditEvent) {}
    pub fn log_auth_success(&self, _user: &str, _mechanism: &str, _client_ip: &SocketAddr) {}
    pub fn log_auth_failure(
        &self,
        _user: Option<&str>,
        _mechanism: &str,
        _client_ip: &SocketAddr,
        _reason: &str,
    ) {
    }
    pub fn log_acl_allow(
        &self,
        _user: &str,
        _resource_type: &str,
        _resource_name: &str,
        _operation: &str,
        _client_ip: &SocketAddr,
    ) {
    }
    pub fn log_acl_deny(
        &self,
        _user: &str,
        _resource_type: &str,
        _resource_name: &str,
        _operation: &str,
        _client_ip: &SocketAddr,
        _reason: &str,
    ) {
    }
    pub fn log_connection(&self, _client_ip: &SocketAddr, _action: &str, _tls_enabled: bool) {}
    pub fn log_connect(&self, _client_ip: &SocketAddr, _tls_enabled: bool) {}
    pub fn log_disconnect(&self, _client_ip: &SocketAddr, _tls_enabled: bool) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.format, "json");
        assert!(config.events.is_empty());
    }

    #[test]
    fn test_audit_logger_disabled() {
        let config = AuditConfig::default();
        let logger = AuditLogger::new(config, 1).unwrap();

        assert!(!logger.is_enabled());

        // Should not panic when logging while disabled
        logger.log(AuditEvent::AuthSuccess {
            user: "test".to_string(),
            mechanism: "PLAIN".to_string(),
            client_ip: "127.0.0.1:12345".to_string(),
        });
    }

    #[test]
    fn test_audit_logger_json_format() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("audit.log");

        let config = AuditConfig {
            enabled: true,
            log_path: Some(log_path.clone()),
            format: "json".to_string(),
            events: vec![],
            log_to_stdout: false,
        };

        let logger = AuditLogger::new(config, 1).unwrap();
        assert!(logger.is_enabled());

        logger.log(AuditEvent::AuthSuccess {
            user: "alice".to_string(),
            mechanism: "SCRAM-SHA-256".to_string(),
            client_ip: "192.168.1.100:54321".to_string(),
        });

        // Read and verify log
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("AUTH_SUCCESS"));
        assert!(contents.contains("alice"));
        assert!(contents.contains("SCRAM-SHA-256"));
    }

    #[test]
    fn test_audit_event_filtering() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("audit.log");

        let config = AuditConfig {
            enabled: true,
            log_path: Some(log_path.clone()),
            format: "json".to_string(),
            events: vec!["AUTH_FAILURE".to_string()], // Only log failures
            log_to_stdout: false,
        };

        let logger = AuditLogger::new(config, 1).unwrap();

        // This should be filtered out
        logger.log(AuditEvent::AuthSuccess {
            user: "alice".to_string(),
            mechanism: "PLAIN".to_string(),
            client_ip: "127.0.0.1:12345".to_string(),
        });

        // This should be logged
        logger.log(AuditEvent::AuthFailure {
            user: Some("bob".to_string()),
            mechanism: "PLAIN".to_string(),
            client_ip: "127.0.0.1:12346".to_string(),
            reason: "invalid password".to_string(),
        });

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(!contents.contains("alice"));
        assert!(contents.contains("bob"));
        assert!(contents.contains("AUTH_FAILURE"));
    }

    #[test]
    fn test_audit_text_format() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("audit.log");

        let config = AuditConfig {
            enabled: true,
            log_path: Some(log_path.clone()),
            format: "text".to_string(),
            events: vec![],
            log_to_stdout: false,
        };

        let logger = AuditLogger::new(config, 1).unwrap();

        logger.log(AuditEvent::TopicCreate {
            user: Some("admin".to_string()),
            topic: "my-topic".to_string(),
            partitions: 3,
            client_ip: "127.0.0.1:12345".to_string(),
        });

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("TOPIC_CREATE"));
        assert!(contents.contains("my-topic"));
        assert!(contents.contains("partitions=3"));
    }

    #[test]
    fn test_audit_log_serialization() {
        let entry = AuditLogEntry {
            timestamp: "2025-01-15T10:30:00Z".to_string(),
            node_id: 1,
            correlation_id: Some("abc-123".to_string()),
            event: AuditEvent::AclDeny {
                user: "test".to_string(),
                resource_type: "Topic".to_string(),
                resource_name: "secret-topic".to_string(),
                operation: "Read".to_string(),
                client_ip: "127.0.0.1:12345".to_string(),
                reason: "no matching ACL".to_string(),
            },
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("ACL_DENY"));
        assert!(json.contains("secret-topic"));
        assert!(json.contains("abc-123"));

        // Verify it can be deserialized
        let _: AuditLogEntry = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_connection_logging() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("audit.log");

        let config = AuditConfig {
            enabled: true,
            log_path: Some(log_path.clone()),
            format: "json".to_string(),
            events: vec!["CONNECTION".to_string()],
            log_to_stdout: false,
        };

        let logger = AuditLogger::new(config, 1).unwrap();

        let addr: SocketAddr = "192.168.1.100:54321".parse().unwrap();

        // Log connect
        logger.log_connect(&addr, true);

        // Log disconnect
        logger.log_disconnect(&addr, true);

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("CONNECTION"));
        assert!(contents.contains("192.168.1.100:54321"));
        assert!(contents.contains("connect"));
        assert!(contents.contains("disconnect"));
        assert!(contents.contains("\"tls_enabled\":true"));
    }

    #[test]
    fn test_connection_logging_text_format() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("audit.log");

        let config = AuditConfig {
            enabled: true,
            log_path: Some(log_path.clone()),
            format: "text".to_string(),
            events: vec![],
            log_to_stdout: false,
        };

        let logger = AuditLogger::new(config, 1).unwrap();

        let addr: SocketAddr = "10.0.0.50:12345".parse().unwrap();
        logger.log_connect(&addr, false);

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("CONNECTION"));
        assert!(contents.contains("10.0.0.50:12345"));
        assert!(contents.contains("action=connect"));
        assert!(contents.contains("tls=false"));
    }
}
