//! Project templates for quickstart wizard
//!
//! Generates complete example projects for each supported SDK language.

/// Generate a Python project with producer and consumer examples.
pub fn generate_python_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "requirements.txt".into(),
            "streamline-sdk>=0.2.0\n".into(),
        ),
        (
            "producer.py".into(),
            format!(
                r#""""Sample Streamline producer."""

import asyncio
import json
import datetime
from streamline import StreamlineProducer


async def main():
    producer = StreamlineProducer(bootstrap_servers="localhost:9092")
    await producer.start()

    try:
        for i in range(10):
            message = {{
                "id": f"msg-{{i:03d}}",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "payload": f"Hello from Python producer #{{i}}",
            }}
            await producer.send("{topic}", json.dumps(message).encode())
            print(f"Produced: {{message['id']}}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
"#
            ),
        ),
        (
            "consumer.py".into(),
            format!(
                r#""""Sample Streamline consumer."""

import asyncio
import json
from streamline import StreamlineConsumer


async def main():
    consumer = StreamlineConsumer(
        bootstrap_servers="localhost:9092",
        group_id="python-demo-group",
        topics=["{topic}"],
    )
    await consumer.start()

    try:
        print(f"Consuming from '{topic}'... (Ctrl+C to stop)")
        async for message in consumer:
            value = json.loads(message.value.decode())
            print(f"[offset={{message.offset}}] {{value}}")
    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline Python Demo

A sample producer/consumer project using the Streamline Python SDK.

## Prerequisites

- Python 3.9+
- Streamline server running on `localhost:9092`

## Setup

```bash
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
# Terminal 1 — start the consumer
python consumer.py

# Terminal 2 — run the producer
python producer.py
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}

/// Generate a TypeScript/Node.js project with producer and consumer examples.
pub fn generate_node_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "package.json".into(),
            format!(
                r#"{{
  "name": "streamline-demo",
  "version": "1.0.0",
  "description": "Streamline Node.js demo project",
  "scripts": {{
    "producer": "npx ts-node producer.ts",
    "consumer": "npx ts-node consumer.ts",
    "build": "tsc"
  }},
  "dependencies": {{
    "@streamlinelabs/streamline-sdk": "^0.2.0"
  }},
  "devDependencies": {{
    "typescript": "^5.3.0",
    "ts-node": "^10.9.0",
    "@types/node": "^20.0.0"
  }}
}}
"#
            ),
        ),
        (
            "tsconfig.json".into(),
            r#"{
  "compilerOptions": {
    "target": "ES2022",
    "module": "commonjs",
    "strict": true,
    "esModuleInterop": true,
    "outDir": "dist",
    "rootDir": ".",
    "skipLibCheck": true,
    "resolveJsonModule": true
  },
  "include": ["*.ts"]
}
"#
            .into(),
        ),
        (
            "producer.ts".into(),
            format!(
                r#"import {{ StreamlineProducer }} from "@streamlinelabs/streamline-sdk";

async function main() {{
  const producer = new StreamlineProducer({{
    brokers: ["localhost:9092"],
  }});

  await producer.connect();

  try {{
    for (let i = 0; i < 10; i++) {{
      const message = {{
        id: `msg-${{String(i).padStart(3, "0")}}`,
        timestamp: new Date().toISOString(),
        payload: `Hello from TypeScript producer #${{i}}`,
      }};

      await producer.send({{
        topic: "{topic}",
        messages: [{{ value: JSON.stringify(message) }}],
      }});

      console.log(`Produced: ${{message.id}}`);
    }}
  }} finally {{
    await producer.disconnect();
  }}
}}

main().catch(console.error);
"#
            ),
        ),
        (
            "consumer.ts".into(),
            format!(
                r#"import {{ StreamlineConsumer }} from "@streamlinelabs/streamline-sdk";

async function main() {{
  const consumer = new StreamlineConsumer({{
    brokers: ["localhost:9092"],
    groupId: "node-demo-group",
  }});

  await consumer.connect();
  await consumer.subscribe({{ topic: "{topic}", fromBeginning: true }});

  console.log(`Consuming from '{topic}'... (Ctrl+C to stop)`);

  await consumer.run({{
    eachMessage: async ({{ message, partition }}) => {{
      const value = JSON.parse(message.value!.toString());
      console.log(`[partition=${{partition}} offset=${{message.offset}}]`, value);
    }},
  }});
}}

main().catch(console.error);
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline Node.js Demo

A sample producer/consumer project using the Streamline Node.js SDK.

## Prerequisites

- Node.js 18+
- Streamline server running on `localhost:9092`

## Setup

```bash
npm install
```

## Run

```bash
# Terminal 1 — start the consumer
npm run consumer

# Terminal 2 — run the producer
npm run producer
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}

/// Generate a Java project with producer and consumer examples.
pub fn generate_java_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "pom.xml".into(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>streamline-demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.streamlinelabs</groupId>
            <artifactId>streamline-client</artifactId>
            <version>0.2.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.1.0</version>
            </plugin>
        </plugins>
    </build>
</project>
"#
            ),
        ),
        (
            "src/main/java/com/example/Producer.java".into(),
            format!(
                r#"package com.example;

import com.streamlinelabs.client.StreamlineProducer;
import com.streamlinelabs.client.ProducerRecord;

import java.time.Instant;

public class Producer {{
    public static void main(String[] args) throws Exception {{
        try (var producer = StreamlineProducer.builder()
                .bootstrapServers("localhost:9092")
                .build()) {{

            for (int i = 0; i < 10; i++) {{
                String id = String.format("msg-%03d", i);
                String value = String.format(
                    "{{\\"id\\": \\"%s\\", \\"timestamp\\": \\"%s\\", \\"payload\\": \\"Hello from Java producer #%d\\"}}",
                    id, Instant.now(), i
                );

                producer.send(new ProducerRecord<>("{topic}", id, value));
                System.out.printf("Produced: %s%n", id);
            }}
        }}
    }}
}}
"#
            ),
        ),
        (
            "src/main/java/com/example/Consumer.java".into(),
            format!(
                r#"package com.example;

import com.streamlinelabs.client.StreamlineConsumer;
import com.streamlinelabs.client.ConsumerRecord;

import java.time.Duration;
import java.util.List;

public class Consumer {{
    public static void main(String[] args) {{
        try (var consumer = StreamlineConsumer.builder()
                .bootstrapServers("localhost:9092")
                .groupId("java-demo-group")
                .build()) {{

            consumer.subscribe(List.of("{topic}"));
            System.out.printf("Consuming from '%s'... (Ctrl+C to stop)%n", "{topic}");

            while (true) {{
                var records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {{
                    System.out.printf("[partition=%d offset=%d] %s%n",
                        record.partition(), record.offset(), record.value());
                }}
            }}
        }}
    }}
}}
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline Java Demo

A sample producer/consumer project using the Streamline Java SDK.

## Prerequisites

- Java 17+
- Maven 3.8+
- Streamline server running on `localhost:9092`

## Build

```bash
mvn compile
```

## Run

```bash
# Terminal 1 — start the consumer
mvn exec:java -Dexec.mainClass="com.example.Consumer"

# Terminal 2 — run the producer
mvn exec:java -Dexec.mainClass="com.example.Producer"
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}

/// Generate a Go project with producer and consumer examples.
pub fn generate_go_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "go.mod".into(),
            r#"module streamline-demo

go 1.21

require github.com/streamlinelabs/streamline-go-sdk v0.2.0
"#
            .into(),
        ),
        (
            "producer/main.go".into(),
            format!(
                r#"package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	streamline "github.com/streamlinelabs/streamline-go-sdk"
)

func main() {{
	producer, err := streamline.NewProducer([]string{{"localhost:9092"}})
	if err != nil {{
		log.Fatal(err)
	}}
	defer producer.Close()

	for i := 0; i < 10; i++ {{
		msg := map[string]interface{{}}{{
			"id":        fmt.Sprintf("msg-%03d", i),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"payload":   fmt.Sprintf("Hello from Go producer #%d", i),
		}}
		value, _ := json.Marshal(msg)

		err := producer.Send("{topic}", value)
		if err != nil {{
			log.Printf("Failed to produce: %v", err)
			continue
		}}
		fmt.Printf("Produced: %s\n", msg["id"])
	}}
}}
"#
            ),
        ),
        (
            "consumer/main.go".into(),
            format!(
                r#"package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	streamline "github.com/streamlinelabs/streamline-go-sdk"
)

func main() {{
	consumer, err := streamline.NewConsumer([]string{{"localhost:9092"}}, "go-demo-group")
	if err != nil {{
		log.Fatal(err)
	}}
	defer consumer.Close()

	err = consumer.Subscribe("{topic}")
	if err != nil {{
		log.Fatal(err)
	}}

	fmt.Printf("Consuming from '%s'... (Ctrl+C to stop)\n", "{topic}")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	for {{
		select {{
		case <-sigchan:
			fmt.Println("\nShutting down...")
			return
		default:
			msg, err := consumer.Poll(1000)
			if err != nil {{
				log.Printf("Poll error: %v", err)
				continue
			}}
			if msg == nil {{
				continue
			}}
			var value map[string]interface{{}}
			json.Unmarshal(msg.Value, &value)
			fmt.Printf("[partition=%d offset=%d] %v\n", msg.Partition, msg.Offset, value)
		}}
	}}
}}
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline Go Demo

A sample producer/consumer project using the Streamline Go SDK.

## Prerequisites

- Go 1.21+
- Streamline server running on `localhost:9092`

## Setup

```bash
go mod tidy
```

## Run

```bash
# Terminal 1 — start the consumer
go run consumer/main.go

# Terminal 2 — run the producer
go run producer/main.go
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}

/// Generate a Rust project with producer and consumer examples.
pub fn generate_rust_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "Cargo.toml".into(),
            r#"[package]
name = "streamline-demo"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "producer"
path = "src/producer.rs"

[[bin]]
name = "consumer"
path = "src/consumer.rs"

[dependencies]
streamline-sdk = "0.2.0"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = "0.4"
"#
            .into(),
        ),
        (
            "src/producer.rs".into(),
            format!(
                r#"use serde::Serialize;
use streamline_sdk::StreamlineProducer;

#[derive(Serialize)]
struct Message {{
    id: String,
    timestamp: String,
    payload: String,
}}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
    let producer = StreamlineProducer::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    for i in 0..10 {{
        let msg = Message {{
            id: format!("msg-{{:03}}", i),
            timestamp: chrono::Utc::now().to_rfc3339(),
            payload: format!("Hello from Rust producer #{{}}", i),
        }};

        producer.send("{topic}", serde_json::to_vec(&msg)?).await?;
        println!("Produced: {{}}", msg.id);
    }}

    producer.flush().await?;
    Ok(())
}}
"#
            ),
        ),
        (
            "src/consumer.rs".into(),
            format!(
                r#"use streamline_sdk::StreamlineConsumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
    let consumer = StreamlineConsumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("rust-demo-group")
        .build()
        .await?;

    consumer.subscribe(&["{topic}"]).await?;
    println!("Consuming from '{topic}'... (Ctrl+C to stop)");

    loop {{
        match consumer.poll(std::time::Duration::from_secs(1)).await? {{
            Some(message) => {{
                let value = String::from_utf8_lossy(&message.value);
                println!(
                    "[partition={{}} offset={{}}] {{}}",
                    message.partition, message.offset, value
                );
            }}
            None => continue,
        }}
    }}
}}
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline Rust Demo

A sample producer/consumer project using the Streamline Rust SDK.

## Prerequisites

- Rust 1.75+
- Streamline server running on `localhost:9092`

## Build

```bash
cargo build
```

## Run

```bash
# Terminal 1 — start the consumer
cargo run --bin consumer

# Terminal 2 — run the producer
cargo run --bin producer
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}

/// Generate a C#/.NET project with producer and consumer examples.
pub fn generate_dotnet_project(topic: &str) -> Vec<(String, String)> {
    vec![
        (
            "StreamlineDemo.csproj".into(),
            r#"<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Streamline.Client" Version="0.2.0" />
    <PackageReference Include="System.Text.Json" Version="8.0.0" />
  </ItemGroup>

</Project>
"#
            .into(),
        ),
        (
            "Program.cs".into(),
            format!(
                r#"using System.Text;
using System.Text.Json;
using Streamline.Client;

if (args.Length > 0 && args[0] == "consume")
{{
    await RunConsumer();
}}
else
{{
    await RunProducer();
}}

async Task RunProducer()
{{
    using var producer = new StreamlineProducer("localhost:9092");
    await producer.ConnectAsync();

    for (int i = 0; i < 10; i++)
    {{
        var message = new
        {{
            id = $"msg-{{i:D3}}",
            timestamp = DateTime.UtcNow.ToString("o"),
            payload = $"Hello from C# producer #{{i}}"
        }};

        var json = JsonSerializer.Serialize(message);
        await producer.SendAsync("{topic}", Encoding.UTF8.GetBytes(json));
        Console.WriteLine($"Produced: {{message.id}}");
    }}
}}

async Task RunConsumer()
{{
    using var consumer = new StreamlineConsumer("localhost:9092", "dotnet-demo-group");
    await consumer.ConnectAsync();
    await consumer.SubscribeAsync("{topic}");

    Console.WriteLine("Consuming from '{topic}'... (Ctrl+C to stop)");

    await foreach (var message in consumer.ConsumeAsync())
    {{
        var value = Encoding.UTF8.GetString(message.Value);
        Console.WriteLine($"[partition={{message.Partition}} offset={{message.Offset}}] {{value}}");
    }}
}}
"#
            ),
        ),
        (
            "README.md".into(),
            format!(
                r#"# Streamline .NET Demo

A sample producer/consumer project using the Streamline .NET SDK.

## Prerequisites

- .NET 8.0+
- Streamline server running on `localhost:9092`

## Build

```bash
dotnet build
```

## Run

```bash
# Terminal 1 — start the consumer
dotnet run -- consume

# Terminal 2 — run the producer
dotnet run
```

The producer sends 10 messages to the `{topic}` topic. The consumer reads and prints them.
"#
            ),
        ),
    ]
}
