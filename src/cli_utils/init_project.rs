//! Project scaffolding for `streamline init`.
//!
//! Generates ready-to-run project templates with:
//! - SDK-specific producer/consumer code
//! - docker-compose.yml with Streamline server
//! - README with getting started instructions

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Available SDK targets for project generation.
#[derive(Debug, Clone, Copy)]
pub enum SdkTarget {
    Python,
    Node,
    Go,
    Java,
    Rust,
    Dotnet,
}

impl SdkTarget {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "python" | "py" => Some(Self::Python),
            "node" | "nodejs" | "typescript" | "ts" | "js" => Some(Self::Node),
            "go" | "golang" => Some(Self::Go),
            "java" => Some(Self::Java),
            "rust" | "rs" => Some(Self::Rust),
            "dotnet" | "csharp" | "cs" => Some(Self::Dotnet),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Python => "python",
            Self::Node => "node",
            Self::Go => "go",
            Self::Java => "java",
            Self::Rust => "rust",
            Self::Dotnet => "dotnet",
        }
    }
}

/// Available project templates.
#[derive(Debug, Clone, Copy)]
pub enum Template {
    Quickstart,
    IotDashboard,
    EventSourcing,
    LogPipeline,
}

impl Template {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "quickstart" | "basic" | "default" => Some(Self::Quickstart),
            "iot" | "iot-dashboard" => Some(Self::IotDashboard),
            "event-sourcing" | "eventsourcing" => Some(Self::EventSourcing),
            "log-pipeline" | "logs" => Some(Self::LogPipeline),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Quickstart => "quickstart",
            Self::IotDashboard => "iot-dashboard",
            Self::EventSourcing => "event-sourcing",
            Self::LogPipeline => "log-pipeline",
        }
    }
}

/// Generate a project scaffold in the given directory.
pub fn generate_project(
    dir: &Path,
    sdk: SdkTarget,
    template: Template,
    project_name: &str,
) -> Result<Vec<String>, String> {
    if dir.exists() && dir.read_dir().map_err(|e| e.to_string())?.next().is_some() {
        return Err(format!("Directory '{}' is not empty", dir.display()));
    }

    fs::create_dir_all(dir).map_err(|e| format!("Failed to create directory: {}", e))?;

    let mut files_created = Vec::new();

    // docker-compose.yml (common to all templates)
    let compose = generate_compose(template);
    write_file(dir, "docker-compose.yml", &compose, &mut files_created)?;

    // README.md
    let readme = generate_readme(sdk, template, project_name);
    write_file(dir, "README.md", &readme, &mut files_created)?;

    // SDK-specific files
    match sdk {
        SdkTarget::Python => generate_python(dir, template, project_name, &mut files_created)?,
        SdkTarget::Node => generate_node(dir, template, project_name, &mut files_created)?,
        SdkTarget::Go => generate_go(dir, template, project_name, &mut files_created)?,
        _ => generate_generic(dir, sdk, template, project_name, &mut files_created)?,
    }

    Ok(files_created)
}

fn write_file(dir: &Path, name: &str, content: &str, created: &mut Vec<String>) -> Result<(), String> {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::write(&path, content).map_err(|e| format!("Failed to write {}: {}", name, e))?;
    created.push(name.to_string());
    Ok(())
}

fn generate_compose(template: Template) -> String {
    let topic = match template {
        Template::Quickstart => "demo-events",
        Template::IotDashboard => "iot-sensors",
        Template::EventSourcing => "domain-events",
        Template::LogPipeline => "app-logs",
    };

    format!(r#"services:
  streamline:
    image: ghcr.io/streamlinelabs/streamline:latest
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      - STREAMLINE_LOG_LEVEL=info
      - STREAMLINE_PLAYGROUND=true
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:9094/health"]
      interval: 5s
      timeout: 3s
      retries: 10
    # Default topic for this template
    # Create additional topics via: streamline-cli topics create {topic}
"#, topic = topic)
}

fn generate_readme(sdk: SdkTarget, template: Template, project_name: &str) -> String {
    let install_cmd = match sdk {
        SdkTarget::Python => "pip install streamline-sdk",
        SdkTarget::Node => "npm install",
        SdkTarget::Go => "go mod tidy",
        SdkTarget::Java => "mvn compile",
        SdkTarget::Rust => "cargo build",
        SdkTarget::Dotnet => "dotnet restore",
    };

    let run_cmd = match sdk {
        SdkTarget::Python => "python producer.py",
        SdkTarget::Node => "npx tsx producer.ts",
        SdkTarget::Go => "go run producer.go",
        SdkTarget::Java => "mvn exec:java",
        SdkTarget::Rust => "cargo run",
        SdkTarget::Dotnet => "dotnet run",
    };

    format!(r#"# {project_name}

Generated by `streamline init --sdk={sdk} --template={template}`.

## Quick Start

```bash
# 1. Start Streamline
docker compose up -d

# 2. Install dependencies
{install_cmd}

# 3. Run the producer
{run_cmd}

# 4. In another terminal, run the consumer
{run_cmd_consumer}
```

## Project Structure

- `docker-compose.yml` — Streamline server configuration
- `producer.*` — Produces messages to the topic
- `consumer.*` — Consumes and processes messages
- `README.md` — This file

## Learn More

- [Streamline Documentation](https://streamlinelabs.dev)
- [{sdk_upper} SDK Reference](https://streamlinelabs.dev/docs/sdks/{sdk})
- [API Reference](https://streamlinelabs.dev/docs/api)
"#,
        project_name = project_name,
        sdk = sdk.name(),
        template = template.name(),
        install_cmd = install_cmd,
        run_cmd = run_cmd,
        run_cmd_consumer = run_cmd.replace("producer", "consumer"),
        sdk_upper = sdk.name().to_uppercase(),
    )
}

fn generate_python(dir: &Path, template: Template, _name: &str, created: &mut Vec<String>) -> Result<(), String> {
    let topic = match template {
        Template::Quickstart => "demo-events",
        Template::IotDashboard => "iot-sensors",
        Template::EventSourcing => "domain-events",
        Template::LogPipeline => "app-logs",
    };

    write_file(dir, "requirements.txt", "streamline-sdk>=0.2.0\n", created)?;

    write_file(dir, "producer.py", &format!(r#""""Streamline producer — {template} template."""
import asyncio
import json
import time
from streamline_sdk import Streamline

TOPIC = "{topic}"

async def main():
    client = Streamline(bootstrap_servers="localhost:9092")

    print(f"Producing messages to '{{TOPIC}}'...")
    for i in range(100):
        message = {{
            "id": i,
            "timestamp": time.time(),
            "value": f"event-{{i}}",
        }}
        await client.produce(TOPIC, value=json.dumps(message))
        print(f"  Produced: {{message}}")
        await asyncio.sleep(0.5)

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
"#, template = template.name(), topic = topic), created)?;

    write_file(dir, "consumer.py", &format!(r#""""Streamline consumer — {template} template."""
import asyncio
from streamline_sdk import Streamline

TOPIC = "{topic}"

async def main():
    client = Streamline(bootstrap_servers="localhost:9092")
    consumer = client.consumer(TOPIC, group_id="my-group")

    print(f"Consuming from '{{TOPIC}}'... (Ctrl+C to stop)")
    async for message in consumer:
        print(f"  offset={{message.offset}} key={{message.key}} value={{message.value}}")

if __name__ == "__main__":
    asyncio.run(main())
"#, template = template.name(), topic = topic), created)?;

    Ok(())
}

fn generate_node(dir: &Path, template: Template, name: &str, created: &mut Vec<String>) -> Result<(), String> {
    let topic = match template {
        Template::Quickstart => "demo-events",
        Template::IotDashboard => "iot-sensors",
        Template::EventSourcing => "domain-events",
        Template::LogPipeline => "app-logs",
    };

    write_file(dir, "package.json", &format!(r#"{{
  "name": "{name}",
  "version": "1.0.0",
  "type": "module",
  "scripts": {{
    "produce": "tsx producer.ts",
    "consume": "tsx consumer.ts"
  }},
  "dependencies": {{
    "@streamlinelabs/streamline-sdk": "^0.2.0"
  }},
  "devDependencies": {{
    "tsx": "^4.0.0",
    "typescript": "^5.3.0"
  }}
}}
"#, name = name), created)?;

    write_file(dir, "producer.ts", &format!(r#"import {{ Streamline }} from '@streamlinelabs/streamline-sdk';

const TOPIC = '{topic}';

async function main() {{
  const client = new Streamline({{ bootstrapServers: 'localhost:9092' }});
  await client.connect();

  console.log(`Producing messages to '${{TOPIC}}'...`);
  for (let i = 0; i < 100; i++) {{
    const message = {{ id: i, timestamp: Date.now(), value: `event-${{i}}` }};
    await client.produce(TOPIC, JSON.stringify(message));
    console.log(`  Produced:`, message);
    await new Promise(r => setTimeout(r, 500));
  }}

  await client.disconnect();
}}

main().catch(console.error);
"#, topic = topic), created)?;

    write_file(dir, "consumer.ts", &format!(r#"import {{ Streamline }} from '@streamlinelabs/streamline-sdk';

const TOPIC = '{topic}';

async function main() {{
  const client = new Streamline({{ bootstrapServers: 'localhost:9092' }});
  await client.connect();

  const consumer = client.consumer(TOPIC, {{ groupId: 'my-group' }});
  console.log(`Consuming from '${{TOPIC}}'... (Ctrl+C to stop)`);

  for await (const message of consumer) {{
    console.log(`  offset=${{message.offset}} key=${{message.key}} value=${{message.value}}`);
  }}
}}

main().catch(console.error);
"#, topic = topic), created)?;

    Ok(())
}

fn generate_go(dir: &Path, template: Template, name: &str, created: &mut Vec<String>) -> Result<(), String> {
    let topic = match template {
        Template::Quickstart => "demo-events",
        Template::IotDashboard => "iot-sensors",
        Template::EventSourcing => "domain-events",
        Template::LogPipeline => "app-logs",
    };

    write_file(dir, "go.mod", &format!(r#"module {name}

go 1.22

require github.com/streamlinelabs/streamline-go-sdk v0.2.0
"#, name = name), created)?;

    write_file(dir, "producer.go", &format!(r#"package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func main() {{
	client, err := streamline.NewClient("localhost:9092")
	if err != nil {{
		panic(err)
	}}
	defer client.Close()

	producer := client.Producer()
	topic := "{topic}"

	fmt.Printf("Producing messages to '%s'...\n", topic)
	for i := 0; i < 100; i++ {{
		msg, _ := json.Marshal(map[string]interface{{}}{{
			"id":        i,
			"timestamp": time.Now().Unix(),
			"value":     fmt.Sprintf("event-%d", i),
		}})
		if err := producer.Send(context.Background(), topic, nil, msg); err != nil {{
			fmt.Printf("  Error: %v\n", err)
			continue
		}}
		fmt.Printf("  Produced: %s\n", msg)
		time.Sleep(500 * time.Millisecond)
	}}
}}
"#, topic = topic), created)?;

    Ok(())
}

fn generate_generic(dir: &Path, sdk: SdkTarget, template: Template, _name: &str, created: &mut Vec<String>) -> Result<(), String> {
    let topic = match template {
        Template::Quickstart => "demo-events",
        Template::IotDashboard => "iot-sensors",
        Template::EventSourcing => "domain-events",
        Template::LogPipeline => "app-logs",
    };

    write_file(dir, &format!("producer.{}", sdk.name()), &format!(
        "// Streamline {sdk} producer — {template} template\n// Topic: {topic}\n// See https://streamlinelabs.dev/docs/sdks/{sdk} for full API reference\n",
        sdk = sdk.name(), template = template.name(), topic = topic
    ), created)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sdk_target_from_str() {
        assert!(SdkTarget::from_str("python").is_some());
        assert!(SdkTarget::from_str("py").is_some());
        assert!(SdkTarget::from_str("node").is_some());
        assert!(SdkTarget::from_str("go").is_some());
        assert!(SdkTarget::from_str("java").is_some());
        assert!(SdkTarget::from_str("rust").is_some());
        assert!(SdkTarget::from_str("dotnet").is_some());
        assert!(SdkTarget::from_str("invalid").is_none());
    }

    #[test]
    fn test_template_from_str() {
        assert!(Template::from_str("quickstart").is_some());
        assert!(Template::from_str("iot").is_some());
        assert!(Template::from_str("event-sourcing").is_some());
        assert!(Template::from_str("log-pipeline").is_some());
        assert!(Template::from_str("unknown").is_none());
    }

    #[test]
    fn test_generate_compose() {
        let compose = generate_compose(Template::Quickstart);
        assert!(compose.contains("streamline"));
        assert!(compose.contains("9092:9092"));
    }

    #[test]
    fn test_generate_readme() {
        let readme = generate_readme(SdkTarget::Python, Template::Quickstart, "my-project");
        assert!(readme.contains("my-project"));
        assert!(readme.contains("pip install"));
    }

    #[test]
    fn test_generate_project_nonempty_dir_fails() {
        let dir = std::env::temp_dir().join("streamline-test-nonempty");
        let _ = fs::create_dir_all(&dir);
        let _ = fs::write(dir.join("existing-file"), "content");
        let result = generate_project(&dir, SdkTarget::Python, Template::Quickstart, "test");
        assert!(result.is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
