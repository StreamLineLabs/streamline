# Streamline CLI Reference

> Status: drafted as part of `pre-dx-cli-help` (cross-cutting prereq).
> Auto-generation from `clap` is tracked under M4 P3 (CLI help → docs site
> sync). Until then, this page is hand-curated; PRs adding a new command
> MUST update this file.

## Conventions

- **Required arg** — `<arg>` (angle brackets).
- **Optional arg** — `[arg]` (square brackets).
- **Repeatable** — `<arg>...`.
- **Default value** — shown as `--flag=<v>  (default: <d>)`.
- **Stability** — every command is annotated with its stability tier
  (`Stable | Beta | Experimental`); see `docs/API_STABILITY.md`.

## Top-Level

```
streamline [GLOBAL FLAGS] <command> [SUBCOMMAND] [ARGS]
```

Global flags accepted by all commands:

| Flag | Description | Default |
|------|-------------|---------|
| `--data-dir <path>` | Server data directory | `./data` |
| `--config <file>` | Config file (YAML) | none |
| `--format <fmt>` | Output format: `table` / `json` / `csv` / `tsv` | `table` |
| `--quiet` | Suppress non-error output | off |
| `-v, --verbose` | Enable debug logging | off |
| `-h, --help` | Show help for command | — |

## Server

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline` (no subcommand) | Stable | Start broker on default port :9092 |
| `streamline serve` | Stable | Explicit `serve` form (preferred in scripts) |
| `streamline doctor` | Beta | Diagnose local install |
| `streamline shell` | Beta | Interactive REPL |

## Topic Management

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline topic list` | Stable | List topics in current data-dir |
| `streamline topic create <name>` | Stable | Create a topic |
| `streamline topic describe <name>` | Stable | Show topic config + partition stats |
| `streamline topic delete <name>` | Stable | Delete a topic |
| `streamline topic config get <name> <key>` | Stable | Read a single config value |
| `streamline topic config set <name> <key> <value>` | Stable | Update a config value |

### Semantic Topic Flags (M2 — Experimental)

These flags are accepted by `streamline topic create` and live behind the
`semantic-topics` feature flag. They will appear under
`streamline topic create --help` only when the broker advertises the
`StreamlineSearch` API key (80, see `docs/protocol-extensions.md`).

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--semantic.embed` | `on \| off` | `off` | Maintain a vector index per partition |
| `--semantic.model` | `<name>` | `bge-small-en-v1.5` | Embedding model alias (see ADR-0014) |
| `--semantic.field` | `<json-path>` | `$` | JSON path within record value to embed |
| `--semantic.cold-tier` | `on \| off` | `off` | Replicate index to cold tier (M2 P2) |

## Producer / Consumer

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline produce <topic> <value>` | Stable | Produce a single record |
| `streamline consume <topic>` | Stable | Tail a topic |
| `streamline groups list` | Stable | List consumer groups |
| `streamline groups offsets <group>` | Stable | Show offsets for a group |

## Cluster

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline cluster status` | Beta | Show Raft leader / followers |
| `streamline cluster join` | Beta | Add a node to the cluster |

## Contracts (M4 — Experimental, post-P1)

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline contract apply -f <file>` | Experimental | Install/update a contract |
| `streamline contract list` | Experimental | List contracts |
| `streamline contract show <name>` | Experimental | Print a contract |
| `streamline contract bypass <topic> --duration=<d>` | Experimental | Time-bounded admin bypass |

## Branches (M5 — Experimental, post-P1)

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline branch create <name> --from <topic>@<offset>` | Experimental | Create a copy-on-write branch |
| `streamline branch list` | Experimental | List branches |
| `streamline branch discard <name>` | Experimental | Delete a branch |
| `streamline branch run <name> --transform <wasm>` | Experimental | Replay a transform onto a branch |

## Edge / Anywhere (M3 — Experimental, post-P1)

| Command | Stability | Purpose |
|---------|-----------|---------|
| `streamline join --token <jwt>` | Experimental | Bootstrap an edge node into a cluster |

## Help Discovery

The CLI follows the Unix help convention:

- `streamline --help` lists top-level commands grouped by area.
- `streamline <command> --help` shows command-specific help.
- `streamline <command> <subcommand> --help` shows the deepest help.

If you cannot find a command, run `streamline doctor` — it prints the
detected version, feature set, server port, and any known mis-config.

## Generating This File

This page is hand-maintained today. The auto-gen pipeline planned under
M4 P3 will:

1. Run `streamline --help-export markdown` (a flag added under
   `pre-dx-cli-help` — currently emits one section per top-level
   subcommand).
2. Diff against this file in CI; fail PR if they drift.

Until then, **adding/renaming a CLI command requires editing this file**.
