//! Simple text-based protocol for Streamline
//!
//! This module provides a Redis RESP-like simple protocol for easy integration
//! from any language without needing Kafka client libraries.
//!
//! ## Protocol Format
//!
//! Commands are line-based text. Each command starts with a verb followed by arguments:
//!
//! ```text
//! PRODUCE topic partition key value
//! CONSUME topic partition offset [count]
//! TOPICS
//! CREATE topic [partitions]
//! DELETE topic
//! OFFSETS topic partition
//! PING
//! QUIT
//! ```
//!
//! ## Response Format
//!
//! Responses follow a simple format:
//! - Success: `+OK` or `+OK <data>`
//! - Error: `-ERR <message>`
//! - Data: `$<length>\r\n<data>\r\n` for binary data
//! - Arrays: `*<count>\r\n<elements>` for multiple items
//! - Integers: `:<number>\r\n`
//!
//! ## Example Session
//!
//! ```text
//! > PING
//! +PONG
//! > CREATE events 3
//! +OK
//! > PRODUCE events 0 user123 {"action":"click"}
//! :0
//! > CONSUME events 0 0 10
//! *1
//! $42
//! {"offset":0,"key":"user123","value":{"action":"click"}}
//! > QUIT
//! +BYE
//! ```

use crate::storage::TopicManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// Simple protocol server state
#[derive(Clone)]
pub struct SimpleProtocolState {
    /// Topic manager for storage operations
    pub topic_manager: Arc<TopicManager>,
}

/// Simple protocol handler
pub struct SimpleProtocolHandler {
    state: SimpleProtocolState,
}

/// Parsed command from client
#[derive(Debug)]
enum Command {
    /// PING - health check
    Ping,
    /// QUIT - close connection
    Quit,
    /// TOPICS - list all topics
    Topics,
    /// CREATE <topic> [partitions] - create a topic
    Create { topic: String, partitions: i32 },
    /// DELETE <topic> - delete a topic
    Delete { topic: String },
    /// PRODUCE <topic> <partition> <key> <value> - produce a message
    Produce {
        topic: String,
        partition: i32,
        key: Option<String>,
        value: String,
    },
    /// CONSUME <topic> <partition> <offset> [count] - consume messages
    Consume {
        topic: String,
        partition: i32,
        offset: i64,
        count: i32,
    },
    /// OFFSETS <topic> <partition> - get earliest and latest offsets
    Offsets { topic: String, partition: i32 },
    /// INFO - get server info
    Info,
    /// HELP - show available commands
    Help,
}

/// Response to send to client
#[allow(dead_code)]
enum Response {
    /// Simple string response (+OK)
    Ok(Option<String>),
    /// Error response (-ERR message)
    Error(String),
    /// Integer response (:number)
    Integer(i64),
    /// Bulk string response ($length\r\ndata\r\n)
    Bulk(String),
    /// Array response (*count\r\nelements)
    Array(Vec<Response>),
    /// Null response ($-1)
    Null,
    /// Connection should close
    Close,
}

impl SimpleProtocolHandler {
    /// Create a new simple protocol handler
    pub fn new(state: SimpleProtocolState) -> Self {
        Self { state }
    }

    /// Handle a client connection
    pub async fn handle_connection(&self, stream: TcpStream) -> crate::error::Result<()> {
        let peer_addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        debug!(peer = %peer_addr, "Simple protocol client connected");

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF - client disconnected
                    debug!(peer = %peer_addr, "Client disconnected");
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    debug!(peer = %peer_addr, command = %trimmed, "Received command");

                    match self.parse_command(trimmed) {
                        Ok(command) => {
                            let response = self.execute_command(command).await;
                            let should_close = matches!(response, Response::Close);

                            if let Err(e) = self.write_response(&mut writer, response).await {
                                error!(peer = %peer_addr, error = %e, "Failed to write response");
                                break;
                            }

                            if should_close {
                                break;
                            }
                        }
                        Err(e) => {
                            let response = Response::Error(e);
                            if let Err(e) = self.write_response(&mut writer, response).await {
                                error!(peer = %peer_addr, error = %e, "Failed to write error response");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "Read error");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Parse a command string into a Command enum
    fn parse_command(&self, input: &str) -> Result<Command, String> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".to_string());
        }

        let cmd = parts[0].to_uppercase();
        match cmd.as_str() {
            "PING" => Ok(Command::Ping),
            "QUIT" | "EXIT" | "CLOSE" => Ok(Command::Quit),
            "TOPICS" | "LIST" => Ok(Command::Topics),
            "INFO" => Ok(Command::Info),
            "HELP" | "?" => Ok(Command::Help),

            "CREATE" => {
                if parts.len() < 2 {
                    return Err("CREATE requires topic name".to_string());
                }
                let topic = parts[1].to_string();
                let partitions = if parts.len() > 2 {
                    parts[2]
                        .parse()
                        .map_err(|_| "Invalid partition count".to_string())?
                } else {
                    1
                };
                Ok(Command::Create { topic, partitions })
            }

            "DELETE" => {
                if parts.len() < 2 {
                    return Err("DELETE requires topic name".to_string());
                }
                Ok(Command::Delete {
                    topic: parts[1].to_string(),
                })
            }

            "PRODUCE" | "PUB" | "SEND" => {
                if parts.len() < 4 {
                    return Err(
                        "PRODUCE requires: topic partition key value (use - for null key)"
                            .to_string(),
                    );
                }
                let topic = parts[1].to_string();
                let partition: i32 = parts[2]
                    .parse()
                    .map_err(|_| "Invalid partition number".to_string())?;
                let key = if parts[3] == "-" || parts[3] == "null" {
                    None
                } else {
                    Some(parts[3].to_string())
                };
                // Value is everything after the key, preserving spaces
                let key_end = input
                    .find(parts[3])
                    .map(|i| i + parts[3].len())
                    .unwrap_or(0);
                let value = input[key_end..].trim().to_string();
                if value.is_empty() {
                    return Err("PRODUCE requires a value".to_string());
                }
                Ok(Command::Produce {
                    topic,
                    partition,
                    key,
                    value,
                })
            }

            "CONSUME" | "SUB" | "FETCH" | "GET" => {
                if parts.len() < 4 {
                    return Err("CONSUME requires: topic partition offset [count]".to_string());
                }
                let topic = parts[1].to_string();
                let partition: i32 = parts[2]
                    .parse()
                    .map_err(|_| "Invalid partition number".to_string())?;
                let offset: i64 = parts[3].parse().map_err(|_| "Invalid offset".to_string())?;
                let count = if parts.len() > 4 {
                    parts[4].parse().map_err(|_| "Invalid count".to_string())?
                } else {
                    10
                };
                Ok(Command::Consume {
                    topic,
                    partition,
                    offset,
                    count,
                })
            }

            "OFFSETS" | "OFFSET" => {
                if parts.len() < 3 {
                    return Err("OFFSETS requires: topic partition".to_string());
                }
                let topic = parts[1].to_string();
                let partition: i32 = parts[2]
                    .parse()
                    .map_err(|_| "Invalid partition number".to_string())?;
                Ok(Command::Offsets { topic, partition })
            }

            _ => Err(format!("Unknown command: {}. Type HELP for commands.", cmd)),
        }
    }

    /// Execute a parsed command
    async fn execute_command(&self, command: Command) -> Response {
        match command {
            Command::Ping => Response::Ok(Some("PONG".to_string())),

            Command::Quit => Response::Close,

            Command::Help => {
                let help = vec![
                    "Available commands:",
                    "  PING                           - Health check",
                    "  TOPICS                         - List all topics",
                    "  CREATE <topic> [partitions]    - Create a topic",
                    "  DELETE <topic>                 - Delete a topic",
                    "  PRODUCE <topic> <part> <key> <value> - Produce message (use - for null key)",
                    "  CONSUME <topic> <part> <offset> [count] - Consume messages",
                    "  OFFSETS <topic> <partition>    - Get earliest/latest offsets",
                    "  INFO                           - Server information",
                    "  QUIT                           - Close connection",
                ];
                Response::Bulk(help.join("\n"))
            }

            Command::Info => {
                let topics = self
                    .state
                    .topic_manager
                    .list_topics()
                    .unwrap_or_default()
                    .len();
                let info = format!(
                    "streamline_version:{}\ntopics:{}\nprotocol:simple/1.0",
                    env!("CARGO_PKG_VERSION"),
                    topics
                );
                Response::Bulk(info)
            }

            Command::Topics => match self.state.topic_manager.list_topics() {
                Ok(topics) => {
                    if topics.is_empty() {
                        Response::Array(vec![])
                    } else {
                        let items: Vec<Response> =
                            topics.into_iter().map(|t| Response::Bulk(t.name)).collect();
                        Response::Array(items)
                    }
                }
                Err(e) => Response::Error(format!("Failed to list topics: {}", e)),
            },

            Command::Create { topic, partitions } => {
                match self.state.topic_manager.create_topic(&topic, partitions) {
                    Ok(_) => Response::Ok(Some(format!(
                        "Created topic '{}' with {} partition(s)",
                        topic, partitions
                    ))),
                    Err(e) => Response::Error(format!("Failed to create topic: {}", e)),
                }
            }

            Command::Delete { topic } => match self.state.topic_manager.delete_topic(&topic) {
                Ok(_) => Response::Ok(Some(format!("Deleted topic '{}'", topic))),
                Err(e) => Response::Error(format!("Failed to delete topic: {}", e)),
            },

            Command::Produce {
                topic,
                partition,
                key,
                value,
            } => {
                use bytes::Bytes;

                let key_bytes = key.map(|k| Bytes::from(k.into_bytes()));
                let value_bytes = Bytes::from(value.into_bytes());

                match self
                    .state
                    .topic_manager
                    .append(&topic, partition, key_bytes, value_bytes)
                {
                    Ok(offset) => Response::Integer(offset),
                    Err(e) => Response::Error(format!("Failed to produce: {}", e)),
                }
            }

            Command::Consume {
                topic,
                partition,
                offset,
                count,
            } => match self
                .state
                .topic_manager
                .read(&topic, partition, offset, count as usize)
            {
                Ok(records) => {
                    if records.is_empty() {
                        Response::Array(vec![])
                    } else {
                        let items: Vec<Response> = records
                            .into_iter()
                            .map(|r| {
                                let mut map = HashMap::new();
                                map.insert("offset", serde_json::json!(r.offset));
                                map.insert("timestamp", serde_json::json!(r.timestamp));
                                map.insert(
                                    "key",
                                    r.key
                                        .map(|k| {
                                            serde_json::json!(
                                                String::from_utf8_lossy(&k).to_string()
                                            )
                                        })
                                        .unwrap_or(serde_json::Value::Null),
                                );
                                // Try to parse value as JSON, fall back to string
                                let value = match serde_json::from_slice(&r.value) {
                                    Ok(v) => v,
                                    Err(_) => {
                                        serde_json::json!(
                                            String::from_utf8_lossy(&r.value).to_string()
                                        )
                                    }
                                };
                                map.insert("value", value);
                                Response::Bulk(serde_json::to_string(&map).unwrap_or_default())
                            })
                            .collect();
                        Response::Array(items)
                    }
                }
                Err(e) => Response::Error(format!("Failed to consume: {}", e)),
            },

            Command::Offsets { topic, partition } => {
                let earliest = self
                    .state
                    .topic_manager
                    .earliest_offset(&topic, partition)
                    .unwrap_or(0);
                let latest = self
                    .state
                    .topic_manager
                    .latest_offset(&topic, partition)
                    .unwrap_or(0);

                let info = format!("earliest:{}\nlatest:{}", earliest, latest);
                Response::Bulk(info)
            }
        }
    }

    /// Write a response to the client
    async fn write_response<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
        response: Response,
    ) -> std::io::Result<()> {
        match response {
            Response::Ok(Some(msg)) => {
                writer.write_all(format!("+{}\r\n", msg).as_bytes()).await?;
            }
            Response::Ok(None) => {
                writer.write_all(b"+OK\r\n").await?;
            }
            Response::Error(msg) => {
                writer
                    .write_all(format!("-ERR {}\r\n", msg).as_bytes())
                    .await?;
            }
            Response::Integer(n) => {
                writer.write_all(format!(":{}\r\n", n).as_bytes()).await?;
            }
            Response::Bulk(data) => {
                writer
                    .write_all(format!("${}\r\n{}\r\n", data.len(), data).as_bytes())
                    .await?;
            }
            Response::Array(items) => {
                writer
                    .write_all(format!("*{}\r\n", items.len()).as_bytes())
                    .await?;
                for item in items {
                    Box::pin(self.write_response(writer, item)).await?;
                }
            }
            Response::Null => {
                writer.write_all(b"$-1\r\n").await?;
            }
            Response::Close => {
                writer.write_all(b"+BYE\r\n").await?;
            }
        }
        writer.flush().await
    }
}

/// Start the simple protocol server
pub async fn start_simple_server(
    addr: std::net::SocketAddr,
    state: SimpleProtocolState,
) -> crate::error::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(addr = %addr, "Simple protocol server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(peer = %peer_addr, "New simple protocol connection");
                let handler = SimpleProtocolHandler::new(state.clone());

                tokio::spawn(async move {
                    if let Err(e) = handler.handle_connection(stream).await {
                        warn!(peer = %peer_addr, error = %e, "Simple protocol connection error");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept simple protocol connection");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        let cmd = handler.parse_command("PING").unwrap();
        assert!(matches!(cmd, Command::Ping));

        let cmd = handler.parse_command("ping").unwrap();
        assert!(matches!(cmd, Command::Ping));
    }

    #[test]
    fn test_parse_create() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        let cmd = handler.parse_command("CREATE events").unwrap();
        match cmd {
            Command::Create { topic, partitions } => {
                assert_eq!(topic, "events");
                assert_eq!(partitions, 1);
            }
            _ => panic!("Expected Create command"),
        }

        let cmd = handler.parse_command("CREATE events 5").unwrap();
        match cmd {
            Command::Create { topic, partitions } => {
                assert_eq!(topic, "events");
                assert_eq!(partitions, 5);
            }
            _ => panic!("Expected Create command"),
        }
    }

    #[test]
    fn test_parse_produce() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        let cmd = handler
            .parse_command("PRODUCE events 0 user123 {\"action\":\"click\"}")
            .unwrap();
        match cmd {
            Command::Produce {
                topic,
                partition,
                key,
                value,
            } => {
                assert_eq!(topic, "events");
                assert_eq!(partition, 0);
                assert_eq!(key, Some("user123".to_string()));
                assert_eq!(value, "{\"action\":\"click\"}");
            }
            _ => panic!("Expected Produce command"),
        }

        // Test null key
        let cmd = handler
            .parse_command("PRODUCE events 0 - hello world")
            .unwrap();
        match cmd {
            Command::Produce { key, value, .. } => {
                assert!(key.is_none());
                assert_eq!(value, "hello world");
            }
            _ => panic!("Expected Produce command"),
        }
    }

    #[test]
    fn test_parse_consume() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        let cmd = handler.parse_command("CONSUME events 0 100").unwrap();
        match cmd {
            Command::Consume {
                topic,
                partition,
                offset,
                count,
            } => {
                assert_eq!(topic, "events");
                assert_eq!(partition, 0);
                assert_eq!(offset, 100);
                assert_eq!(count, 10); // default
            }
            _ => panic!("Expected Consume command"),
        }

        let cmd = handler.parse_command("CONSUME events 0 100 50").unwrap();
        match cmd {
            Command::Consume { count, .. } => {
                assert_eq!(count, 50);
            }
            _ => panic!("Expected Consume command"),
        }
    }

    #[test]
    fn test_parse_errors() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        assert!(handler.parse_command("CREATE").is_err());
        assert!(handler.parse_command("PRODUCE events").is_err());
        assert!(handler.parse_command("CONSUME events").is_err());
        assert!(handler.parse_command("UNKNOWN_CMD").is_err());
    }

    #[tokio::test]
    async fn test_execute_ping() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        let response = handler.execute_command(Command::Ping).await;
        match response {
            Response::Ok(Some(msg)) => assert_eq!(msg, "PONG"),
            _ => panic!("Expected Ok response with PONG"),
        }
    }

    #[tokio::test]
    async fn test_execute_create_and_list() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        // Create topic
        let response = handler
            .execute_command(Command::Create {
                topic: "test-topic".to_string(),
                partitions: 3,
            })
            .await;
        assert!(matches!(response, Response::Ok(_)));

        // List topics
        let response = handler.execute_command(Command::Topics).await;
        match response {
            Response::Array(items) => {
                assert_eq!(items.len(), 1);
            }
            _ => panic!("Expected Array response"),
        }
    }

    #[tokio::test]
    async fn test_execute_produce_consume() {
        let state = SimpleProtocolState {
            topic_manager: Arc::new(TopicManager::in_memory().unwrap()),
        };
        let handler = SimpleProtocolHandler::new(state);

        // Create topic first
        handler
            .execute_command(Command::Create {
                topic: "events".to_string(),
                partitions: 1,
            })
            .await;

        // Produce message
        let response = handler
            .execute_command(Command::Produce {
                topic: "events".to_string(),
                partition: 0,
                key: Some("key1".to_string()),
                value: "hello".to_string(),
            })
            .await;
        match response {
            Response::Integer(offset) => assert_eq!(offset, 0),
            _ => panic!("Expected Integer response with offset"),
        }

        // Consume message
        let response = handler
            .execute_command(Command::Consume {
                topic: "events".to_string(),
                partition: 0,
                offset: 0,
                count: 10,
            })
            .await;
        match response {
            Response::Array(items) => {
                assert_eq!(items.len(), 1);
            }
            _ => panic!("Expected Array response"),
        }
    }
}
