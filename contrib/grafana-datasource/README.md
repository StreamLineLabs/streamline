# Streamline StreamQL Grafana Datasource Plugin

A Grafana datasource plugin for querying Streamline servers with StreamQL.

## Features

- Execute StreamQL queries with time-series and table formats
- Macro support: `$__timeFrom`, `$__timeTo`, `$__interval`, `$__timeGroup(col, interval)`
- Pre-built example queries for common use cases
- Alerting and streaming support
- API key authentication

## Installation

### From Grafana Marketplace

Search for "Streamline StreamQL" in Grafana → Configuration → Plugins.

### Manual Installation

```bash
cd /var/lib/grafana/plugins
git clone https://github.com/streamlinelabs/streamline
cd streamline/contrib/grafana-datasource
npm install && npm run build
# Restart Grafana
```

## Configuration

1. Go to Grafana → Configuration → Data Sources → Add data source
2. Select "Streamline StreamQL"
3. Set the **Streamline URL** (e.g., `http://localhost:9094`)
4. Optionally set an **API Key** for authentication
5. Click "Save & Test"

## Dashboard Templates

Pre-built dashboards are available in `dashboards/`:

- **Cluster Overview** — Messages/bytes throughput, consumer lag, latency p50/p99, anomaly alerts

Import via Grafana → Dashboards → Import → Upload JSON.

## Development

```bash
npm install
npm run dev      # Watch mode
npm run build    # Production build
npm run lint     # ESLint
npm run typecheck # TypeScript check
```

## Query Macros

| Macro | Expansion | Description |
|-------|-----------|-------------|
| `$__timeFrom` | `'2024-01-01T00:00:00Z'` | Dashboard time range start |
| `$__timeTo` | `'2024-01-01T01:00:00Z'` | Dashboard time range end |
| `$__interval` | `'60000ms'` | Auto-calculated interval |
| `$__timeGroup(col, interval)` | `time_bucket(interval, col)` | Time bucketing |
