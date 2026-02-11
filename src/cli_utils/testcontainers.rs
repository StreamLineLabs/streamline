//! Testcontainers & CI Integration support for Streamline
//!
//! Provides configuration generators for:
//! - Testcontainers (Java, Python, Go, .NET)
//! - GitHub Actions
//! - Docker Compose for testing
//! - `streamline dev` configuration presets

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Testcontainers configuration for various languages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestcontainersConfig {
    /// Docker image name
    pub image: String,
    /// Image tag
    pub tag: String,
    /// Exposed ports
    pub exposed_ports: Vec<PortMapping>,
    /// Environment variables
    pub env_vars: HashMap<String, String>,
    /// Health check command
    pub health_check: HealthCheck,
    /// Startup timeout in seconds
    pub startup_timeout_secs: u32,
    /// Wait strategy
    pub wait_strategy: WaitStrategy,
}

/// Port mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortMapping {
    pub container_port: u16,
    pub protocol: String,
    pub description: String,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub endpoint: String,
    pub interval_secs: u32,
    pub retries: u32,
}

/// Container wait strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WaitStrategy {
    /// Wait for a log message
    LogMessage { pattern: String },
    /// Wait for HTTP endpoint to return 200
    Http { path: String, port: u16 },
    /// Wait for port to be open
    Port { port: u16 },
}

impl Default for TestcontainersConfig {
    fn default() -> Self {
        Self {
            image: "streamline/streamline".to_string(),
            tag: "latest".to_string(),
            exposed_ports: vec![
                PortMapping {
                    container_port: 9092,
                    protocol: "tcp".to_string(),
                    description: "Kafka protocol".to_string(),
                },
                PortMapping {
                    container_port: 9094,
                    protocol: "tcp".to_string(),
                    description: "HTTP API".to_string(),
                },
            ],
            env_vars: {
                let mut env = HashMap::new();
                env.insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
                env.insert("STREAMLINE_LOG_LEVEL".to_string(), "warn".to_string());
                env
            },
            health_check: HealthCheck {
                endpoint: "http://localhost:9094/health".to_string(),
                interval_secs: 2,
                retries: 15,
            },
            startup_timeout_secs: 30,
            wait_strategy: WaitStrategy::Http {
                path: "/health".to_string(),
                port: 9094,
            },
        }
    }
}

impl TestcontainersConfig {
    /// Generate Java Testcontainers code
    pub fn generate_java(&self) -> String {
        format!(
            r#"// Streamline Testcontainer for Java
// Add to pom.xml: org.testcontainers:testcontainers:1.19+

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class StreamlineContainer extends GenericContainer<StreamlineContainer> {{

    private static final int KAFKA_PORT = {kafka_port};
    private static final int HTTP_PORT = {http_port};

    public StreamlineContainer() {{
        this("{image}:{tag}");
    }}

    public StreamlineContainer(String dockerImageName) {{
        super(DockerImageName.parse(dockerImageName));
        withExposedPorts(KAFKA_PORT, HTTP_PORT);
        withEnv("STREAMLINE_IN_MEMORY", "true");
        withEnv("STREAMLINE_LOG_LEVEL", "warn");
        waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT));
        withStartupTimeout(java.time.Duration.ofSeconds({timeout}));
    }}

    public String getBootstrapServers() {{
        return getHost() + ":" + getMappedPort(KAFKA_PORT);
    }}

    public String getHttpUrl() {{
        return "http://" + getHost() + ":" + getMappedPort(HTTP_PORT);
    }}
}}"#,
            kafka_port = 9092,
            http_port = 9094,
            image = self.image,
            tag = self.tag,
            timeout = self.startup_timeout_secs,
        )
    }

    /// Generate Python Testcontainers code
    pub fn generate_python(&self) -> String {
        format!(
            r#"# Streamline Testcontainer for Python
# pip install testcontainers

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import requests
import time


class StreamlineContainer(DockerContainer):
    """Streamline container for testing with Kafka-compatible clients."""

    KAFKA_PORT = {kafka_port}
    HTTP_PORT = {http_port}

    def __init__(self, image="{image}:{tag}", **kwargs):
        super().__init__(image, **kwargs)
        self.with_exposed_ports(self.KAFKA_PORT, self.HTTP_PORT)
        self.with_env("STREAMLINE_IN_MEMORY", "true")
        self.with_env("STREAMLINE_LOG_LEVEL", "warn")

    def start(self):
        super().start()
        self._wait_for_ready()
        return self

    def _wait_for_ready(self, timeout={timeout}):
        start = time.time()
        while time.time() - start < timeout:
            try:
                url = f"http://{{self.get_container_host_ip()}}:{{self.get_exposed_port(self.HTTP_PORT)}}/health"
                resp = requests.get(url, timeout=2)
                if resp.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise TimeoutError("Streamline container did not become ready")

    @property
    def bootstrap_servers(self):
        return f"{{self.get_container_host_ip()}}:{{self.get_exposed_port(self.KAFKA_PORT)}}"

    @property
    def http_url(self):
        return f"http://{{self.get_container_host_ip()}}:{{self.get_exposed_port(self.HTTP_PORT)}}"


# Usage:
# with StreamlineContainer() as streamline:
#     producer = KafkaProducer(bootstrap_servers=streamline.bootstrap_servers)
#     producer.send("test-topic", b"hello")
"#,
            kafka_port = 9092,
            http_port = 9094,
            image = self.image,
            tag = self.tag,
            timeout = self.startup_timeout_secs,
        )
    }

    /// Generate Go Testcontainers code
    pub fn generate_go(&self) -> String {
        format!(
            r#"// Streamline Testcontainer for Go
// go get github.com/testcontainers/testcontainers-go

package streamline

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	kafkaPort = "{kafka_port}/tcp"
	httpPort  = "{http_port}/tcp"
)

type StreamlineContainer struct {{
	testcontainers.Container
}}

func StartStreamline(ctx context.Context) (*StreamlineContainer, error) {{
	req := testcontainers.ContainerRequest{{
		Image:        "{image}:{tag}",
		ExposedPorts: []string{{kafkaPort, httpPort}},
		Env: map[string]string{{
			"STREAMLINE_IN_MEMORY": "true",
			"STREAMLINE_LOG_LEVEL": "warn",
		}},
		WaitingFor: wait.ForHTTP("/health").
			WithPort(httpPort).
			WithStartupTimeout({timeout} * time.Second),
	}}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{{
		ContainerRequest: req,
		Started:          true,
	}})
	if err != nil {{
		return nil, err
	}}

	return &StreamlineContainer{{Container: container}}, nil
}}

func (c *StreamlineContainer) BootstrapServers(ctx context.Context) (string, error) {{
	host, err := c.Host(ctx)
	if err != nil {{
		return "", err
	}}
	port, err := c.MappedPort(ctx, kafkaPort)
	if err != nil {{
		return "", err
	}}
	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}}
"#,
            kafka_port = 9092,
            http_port = 9094,
            image = self.image,
            tag = self.tag,
            timeout = self.startup_timeout_secs,
        )
    }

    /// Generate GitHub Actions workflow snippet
    pub fn generate_github_action(&self) -> String {
        format!(
            r#"# Streamline GitHub Action
# Add this to .github/workflows/test.yml

name: Tests with Streamline
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      streamline:
        image: {image}:{tag}
        ports:
          - 9092:9092
          - 9094:9094
        env:
          STREAMLINE_IN_MEMORY: "true"
          STREAMLINE_PLAYGROUND: "true"
        options: >-
          --health-cmd "curl -f http://localhost:9094/health || exit 1"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10

    steps:
      - uses: actions/checkout@v4

      - name: Wait for Streamline
        run: |
          for i in $(seq 1 30); do
            curl -sf http://localhost:9094/health && break
            sleep 1
          done

      - name: Run tests
        env:
          KAFKA_BOOTSTRAP_SERVERS: localhost:9092
          STREAMLINE_HTTP_URL: http://localhost:9094
        run: |
          # Your test command here
          echo "Streamline is ready at localhost:9092"
"#,
            image = self.image,
            tag = self.tag,
        )
    }

    /// Generate Docker Compose for testing
    pub fn generate_docker_compose(&self) -> String {
        format!(
            r#"# Streamline Docker Compose for Testing
# docker compose -f docker-compose.test.yml up -d

version: "3.8"

services:
  streamline:
    image: {image}:{tag}
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      STREAMLINE_IN_MEMORY: "true"
      STREAMLINE_PLAYGROUND: "true"
      STREAMLINE_LOG_LEVEL: info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9094/health"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 5s
"#,
            image = self.image,
            tag = self.tag,
        )
    }
}

/// Dev mode presets for `streamline dev`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevPreset {
    /// Minimal: in-memory, single partition topics
    Minimal,
    /// Playground: demo topics with mock data
    Playground,
    /// Integration: multi-partition, persistent storage
    Integration,
    /// Performance: tuned for benchmarking
    Performance,
}

impl DevPreset {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Minimal => "In-memory, single partition, zero config",
            Self::Playground => "Demo topics with generated mock data",
            Self::Integration => "Multi-partition, persistent, realistic config",
            Self::Performance => "Tuned for benchmarking with high throughput",
        }
    }

    pub fn env_vars(&self) -> HashMap<String, String> {
        let mut vars = HashMap::new();
        match self {
            Self::Minimal => {
                vars.insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
                vars.insert("STREAMLINE_LOG_LEVEL".to_string(), "warn".to_string());
            }
            Self::Playground => {
                vars.insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
                vars.insert("STREAMLINE_PLAYGROUND".to_string(), "true".to_string());
            }
            Self::Integration => {
                vars.insert("STREAMLINE_LOG_LEVEL".to_string(), "info".to_string());
            }
            Self::Performance => {
                vars.insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
                vars.insert("STREAMLINE_LOG_LEVEL".to_string(), "error".to_string());
                vars.insert("STREAMLINE_RUNTIME_MODE".to_string(), "sharded".to_string());
            }
        }
        vars
    }
}

impl std::fmt::Display for DevPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Minimal => write!(f, "minimal"),
            Self::Playground => write!(f, "playground"),
            Self::Integration => write!(f, "integration"),
            Self::Performance => write!(f, "performance"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TestcontainersConfig::default();
        assert_eq!(config.image, "streamline/streamline");
        assert_eq!(config.exposed_ports.len(), 2);
        assert_eq!(config.exposed_ports[0].container_port, 9092);
    }

    #[test]
    fn test_generate_java() {
        let config = TestcontainersConfig::default();
        let java = config.generate_java();
        assert!(java.contains("StreamlineContainer"));
        assert!(java.contains("9092"));
        assert!(java.contains("getBootstrapServers"));
    }

    #[test]
    fn test_generate_python() {
        let config = TestcontainersConfig::default();
        let python = config.generate_python();
        assert!(python.contains("StreamlineContainer"));
        assert!(python.contains("bootstrap_servers"));
    }

    #[test]
    fn test_generate_go() {
        let config = TestcontainersConfig::default();
        let go_code = config.generate_go();
        assert!(go_code.contains("StreamlineContainer"));
        assert!(go_code.contains("BootstrapServers"));
    }

    #[test]
    fn test_generate_github_action() {
        let config = TestcontainersConfig::default();
        let action = config.generate_github_action();
        assert!(action.contains("streamline/streamline"));
        assert!(action.contains("health-cmd"));
    }

    #[test]
    fn test_generate_docker_compose() {
        let config = TestcontainersConfig::default();
        let compose = config.generate_docker_compose();
        assert!(compose.contains("9092:9092"));
        assert!(compose.contains("healthcheck"));
    }

    #[test]
    fn test_dev_presets() {
        assert_eq!(DevPreset::Minimal.to_string(), "minimal");
        assert!(DevPreset::Playground
            .env_vars()
            .contains_key("STREAMLINE_PLAYGROUND"));
        assert!(DevPreset::Performance
            .env_vars()
            .contains_key("STREAMLINE_RUNTIME_MODE"));
    }

    #[test]
    fn test_dev_preset_descriptions() {
        assert!(!DevPreset::Minimal.description().is_empty());
        assert!(!DevPreset::Playground.description().is_empty());
    }
}
