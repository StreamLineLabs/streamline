# Cross-SDK Integration Tests

Validates that all Streamline SDKs work correctly against the Streamline server by testing the core Kafka protocol operations every SDK relies on.

## Test Layers

### 1. Protocol Compatibility Tests (Rust, no Docker)

Fast, in-process tests that verify the core storage and protocol operations using the embedded `TopicManager`, `GroupCoordinator`, and `EmbeddedStreamline` APIs.

**What is tested:**

| Category | Tests |
|---|---|
| Produce/Fetch | Single message, multi-partition, null key, seek, headers, batch, large (1MB) |
| Consumer Groups | Offset commit/fetch, auto-commit, independent groups, coordinator lifecycle |
| Topic Management | Create, list, describe, delete, auto-create, duplicate detection |
| Metadata & Offsets | Earliest/latest offset, high watermark, topic metadata |
| API Versions | Encode/decode ApiVersionsRequest/Response with expected API keys |
| Error Handling | Non-existent topics, invalid partitions, duplicate creates, invalid offsets |
| Persistence | Data survives embedded instance restart |
| Record Management | Delete records before offset |

**Run:**

```bash
cargo test --test cross_sdk_test
```

Or via Make:

```bash
make test-cross-sdk
```

### 2. Full Multi-SDK Integration Tests (Docker)

Runs actual SDK test suites against a live Streamline server container. Each SDK container connects to the server over the network and runs its integration tests.

**SDKs tested:**
- Java (Maven)
- Python (pytest)
- Go (go test)
- Node.js (vitest)
- Rust (cargo test)
- .NET (dotnet test)

**Prerequisites:**
- Docker and Docker Compose installed
- Each SDK repo must have a `Dockerfile.test` that runs its integration tests
- The `STREAMLINE_BOOTSTRAP` environment variable is set to `streamline:9092` inside each container

**Run:**

```bash
docker compose -f tests/cross_sdk/docker_compose.yml up --build --abort-on-container-exit
```

Or via Make:

```bash
make test-cross-sdk-full
```

## Adding a New SDK

1. Create a `Dockerfile.test` in the SDK repo that installs dependencies and runs tests.
2. Ensure tests read the `STREAMLINE_BOOTSTRAP` env var for the server address.
3. Add a new service to `docker_compose.yml` following the existing pattern.
