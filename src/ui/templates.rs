//! HTML templates for the web UI.
//!
//! Uses embedded templates with simple string interpolation for a lightweight
//! approach that doesn't require external template dependencies.

use super::client::{
    BenchmarkStatus, BenchmarkType, BrokerInfo, ClusterOverview, ConsumeResponse,
    ConsumerGroupDetails, ConsumerGroupInfo, ConsumerGroupLag, HeatmapData, LagSeverity,
    ListBenchmarksResponse, TopicComparison, TopicDetails, TopicInfo,
};

/// Template renderer.
pub struct Templates;

impl Templates {
    /// Create a new template renderer.
    pub fn new() -> Self {
        Self
    }

    /// Render the base layout with content.
    pub fn layout(&self, title: &str, content: &str, active_page: &str) -> String {
        let nav = self.nav(active_page);
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Streamline</title>
    <style>
        {css}
    </style>
</head>
<body>
    <div class="container">
        <nav class="sidebar">
            <div class="logo">
                <h1>Streamline</h1>
                <span class="subtitle">The Redis of Streaming</span>
            </div>
            {nav}
        </nav>
        <main class="content">
            <header class="header">
                <h2>{title}</h2>
                <div class="header-controls">
                    <div class="connection-status" id="connection-status">
                        <span class="connection-dot connecting" id="connection-dot"></span>
                        <span id="connection-text">Connecting...</span>
                    </div>
                    <div class="refresh-toggle">
                        <span>Auto-refresh</span>
                        <div class="toggle-switch active" id="auto-refresh-toggle" onclick="toggleAutoRefresh()"></div>
                    </div>
                    <div class="theme-selector">
                        <select id="theme-select" onchange="setTheme(this.value)" title="Select theme">
                            <option value="dark">🌙 Dark</option>
                            <option value="light">☀️ Light</option>
                            <option value="high-contrast">◐ High Contrast</option>
                            <option value="colorblind">👁️ Colorblind</option>
                            <option value="solarized-light">🌅 Solarized Light</option>
                            <option value="solarized-dark">🌃 Solarized Dark</option>
                        </select>
                    </div>
                </div>
            </header>
            <div class="main-content">
                {content}
            </div>
        </main>
    </div>
    <div id="toast-container" class="toast-container"></div>
    <!-- Command Palette Modal -->
    <div id="command-palette" class="command-palette-overlay hidden">
        <div class="command-palette-modal">
            <div class="command-palette-header">
                <input type="text" id="command-palette-input" class="command-palette-input"
                       placeholder="Search topics, groups, actions..." autocomplete="off" spellcheck="false">
                <span class="command-palette-hint">ESC to close</span>
            </div>
            <div id="command-palette-results" class="command-palette-results">
                <div class="command-palette-empty">
                    <span class="muted">Type to search topics, groups, or actions...</span>
                </div>
            </div>
        </div>
    </div>
    <div id="shortcuts-modal" class="shortcuts-modal hidden">
        <div class="shortcuts-content">
            <h3>Keyboard Shortcuts</h3>
            <div class="shortcut-item">
                <span class="shortcut-desc">Command palette</span>
                <span class="shortcut-key">Cmd+K</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Focus search</span>
                <span class="shortcut-key">/</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Show shortcuts</span>
                <span class="shortcut-key">?</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Refresh page</span>
                <span class="shortcut-key">r</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Close modal</span>
                <span class="shortcut-key">Esc</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Go to Dashboard</span>
                <span class="shortcut-key">g d</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Go to Topics</span>
                <span class="shortcut-key">g t</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Go to Consumer Groups</span>
                <span class="shortcut-key">g c</span>
            </div>
            <div class="shortcut-item">
                <span class="shortcut-desc">Go to Brokers</span>
                <span class="shortcut-key">g b</span>
            </div>
        </div>
    </div>
    <script>
        {js}
    </script>
</body>
</html>"#,
            title = title,
            css = CSS,
            nav = nav,
            content = content,
            js = JS,
        )
    }

    /// Render the navigation.
    fn nav(&self, active: &str) -> String {
        let items = [
            ("dashboard", "Dashboard", "/"),
            ("topics", "Topics", "/topics"),
            ("consumer-groups", "Consumer Groups", "/consumer-groups"),
            ("schemas", "Schemas", "/schemas"),
            ("clusters", "Clusters", "/clusters"),
            ("rebalances", "Rebalances", "/rebalances"),
            ("lag-heatmap", "Lag Heatmap", "/lag-heatmap"),
            ("brokers", "Brokers", "/brokers"),
            ("metrics", "Metrics", "/metrics"),
            ("logs", "Logs", "/logs"),
            ("connections", "Connections", "/connections"),
            ("alerts", "Alerts", "/alerts"),
            ("benchmark", "Benchmark", "/benchmark"),
        ];

        let links: Vec<String> = items
            .iter()
            .map(|(id, label, href)| {
                let class = if *id == active { "active" } else { "" };
                format!(
                    r#"<a href="{href}" class="nav-link {class}" data-page="{id}">{label}</a>"#,
                    href = href,
                    class = class,
                    id = id,
                    label = label,
                )
            })
            .collect();

        format!(r#"<div class="nav-links">{}</div>"#, links.join("\n"))
    }

    /// Render the dashboard page.
    pub fn dashboard(&self, overview: &ClusterOverview) -> String {
        let content = format!(
            r#"
<!-- SVG gradient definition for sparklines -->
<svg width="0" height="0" style="position: absolute;">
    <defs>
        <linearGradient id="sparkline-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color: var(--accent-primary); stop-opacity: 0.4"/>
            <stop offset="100%" style="stop-color: var(--accent-primary); stop-opacity: 0"/>
        </linearGradient>
        <linearGradient id="sparkline-gradient-green" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color: var(--success); stop-opacity: 0.4"/>
            <stop offset="100%" style="stop-color: var(--success); stop-opacity: 0"/>
        </linearGradient>
    </defs>
</svg>

<div class="stats-grid">
    <div class="stat-card">
        <div class="stat-label">Brokers</div>
        <div class="stat-value">{broker_count}</div>
        <div class="stat-detail">Controller: {controller}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Topics</div>
        <div class="stat-value">{topic_count}</div>
        <div class="stat-detail">{partition_count} partitions</div>
    </div>
    <div class="stat-card stat-card-with-chart">
        <div class="stat-label">Messages/sec</div>
        <div class="stat-value" id="messages-per-sec">{messages_per_sec:.1}</div>
        <div class="stat-detail">Total: {total_messages}</div>
        <div class="sparkline-container">
            <svg class="sparkline" id="messages-sparkline" viewBox="0 0 100 40" preserveAspectRatio="none">
                <path class="sparkline-area" d="M0,40 L0,40 L100,40 Z"></path>
                <path class="sparkline-line" d="M0,40 L100,40"></path>
                <circle class="sparkline-dot" cx="100" cy="40" r="3"></circle>
            </svg>
        </div>
    </div>
    <div class="stat-card stat-card-with-chart">
        <div class="stat-label">Throughput</div>
        <div class="stat-value" id="bytes-in">{bytes_in}</div>
        <div class="stat-detail">Out: <span id="bytes-out">{bytes_out}</span></div>
        <div class="sparkline-container">
            <svg class="sparkline" id="throughput-sparkline" viewBox="0 0 100 40" preserveAspectRatio="none">
                <path class="sparkline-area" style="fill: url(#sparkline-gradient-green);" d="M0,40 L0,40 L100,40 Z"></path>
                <path class="sparkline-line" style="stroke: var(--success);" d="M0,40 L100,40"></path>
                <circle class="sparkline-dot" style="fill: var(--success);" cx="100" cy="40" r="3"></circle>
            </svg>
        </div>
    </div>
</div>

<div class="dashboard-grid">
    <div class="card">
        <div class="card-header">
            <h3>Cluster Health</h3>
        </div>
        <div class="card-body">
            <div class="health-indicator {health_class}">
                <span class="health-dot"></span>
                <span class="health-text">{health_text}</span>
            </div>
            <div class="health-details">
                <div class="health-item">
                    <span>Online Partitions</span>
                    <span>{online_partitions}</span>
                </div>
                <div class="health-item">
                    <span>Offline Partitions</span>
                    <span class="{offline_class}">{offline_partitions}</span>
                </div>
                <div class="health-item">
                    <span>Under-replicated</span>
                    <span class="{under_replicated_class}">{under_replicated}</span>
                </div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <h3>Recent Activity</h3>
        </div>
        <div class="card-body">
            <div id="activity-feed" class="activity-feed">
                <p class="muted">Connecting to event stream...</p>
            </div>
        </div>
    </div>
</div>
"#,
            broker_count = overview.broker_count,
            controller = overview
                .controller_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            topic_count = overview.topic_count,
            partition_count = overview.partition_count,
            messages_per_sec = overview.messages_per_second,
            total_messages = format_number(overview.total_messages),
            bytes_in = format_bytes_per_sec(overview.bytes_in_per_second),
            bytes_out = format_bytes_per_sec(overview.bytes_out_per_second),
            health_class = if overview.offline_partition_count == 0 {
                "healthy"
            } else {
                "unhealthy"
            },
            health_text = if overview.offline_partition_count == 0 {
                "Healthy"
            } else {
                "Degraded"
            },
            online_partitions = overview.online_partition_count,
            offline_partitions = overview.offline_partition_count,
            offline_class = if overview.offline_partition_count > 0 {
                "error"
            } else {
                ""
            },
            under_replicated = overview.under_replicated_partitions,
            under_replicated_class = if overview.under_replicated_partitions > 0 {
                "warning"
            } else {
                ""
            },
        );

        self.layout("Dashboard", &content, "dashboard")
    }

    /// Render the topics list page.
    pub fn topics_list(&self, topics: &[TopicInfo]) -> String {
        let rows: Vec<String> = topics
            .iter()
            .map(|t| {
                format!(
                    r#"<tr class="vim-item">
                        <td>
                            <button class="favorite-btn" data-type="topic" data-id="{name}" onclick="toggleFavorite('topic', '{name}')">☆</button>
                            <a href="/topics/{name}">{name}</a>
                        </td>
                        <td>{partitions}</td>
                        <td>{replication}</td>
                        <td>{messages}</td>
                        <td>{size}</td>
                        <td>{internal}</td>
                    </tr>"#,
                    name = t.name,
                    partitions = t.partition_count,
                    replication = t.replication_factor,
                    messages = format_number(t.total_messages),
                    size = format_bytes(t.total_bytes),
                    internal = if t.is_internal { "Yes" } else { "No" },
                )
            })
            .collect();

        let empty_state = if topics.is_empty() {
            r#"<tr><td colspan="6">
                <div class="empty-state-container">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-title">No topics yet</div>
                    <div class="empty-state-desc">Create your first topic to start streaming messages</div>
                    <button class="btn btn-primary" onclick="showCreateTopicModal()">Create Topic</button>
                </div>
            </td></tr>"#
        } else {
            ""
        };

        let content = format!(
            r#"
<div class="actions-bar">
    <div class="search-wrapper">
        <span class="search-icon">&#128269;</span>
        <input type="text" class="search-input" id="topics-search"
               placeholder="Search topics..." onkeyup="filterTable('topics-table', this.value)">
        <span class="search-shortcut">/</span>
    </div>
    <button class="btn btn-primary" onclick="showCreateTopicModal()">Create Topic</button>
</div>

<div class="card">
    <table class="data-table">
        <thead>
            <tr>
                <th>Name</th>
                <th>Partitions</th>
                <th>Replication Factor</th>
                <th>Messages</th>
                <th>Size</th>
                <th>Internal</th>
            </tr>
        </thead>
        <tbody id="topics-table" class="vim-navigable">
            {rows}
            {empty}
        </tbody>
    </table>
    <div class="pagination" id="topics-pagination">
        <span class="pagination-info">Showing all items</span>
        <div class="pagination-controls">
            <button class="pagination-btn prev" onclick="changePage('topics-table', -1)" disabled>← Prev</button>
            <span class="pagination-current">Page 1</span>
            <button class="pagination-btn next" onclick="changePage('topics-table', 1)">Next →</button>
            <select class="pagination-select" onchange="changePageSize('topics-table', this.value)">
                <option value="10">10 / page</option>
                <option value="25">25 / page</option>
                <option value="50">50 / page</option>
                <option value="100">100 / page</option>
            </select>
        </div>
    </div>
</div>

<div id="create-topic-modal" class="modal hidden">
    <div class="modal-content">
        <div class="modal-header">
            <h3>Create Topic</h3>
            <button class="close-btn" onclick="hideModal('create-topic-modal')">&times;</button>
        </div>
        <form id="create-topic-form" onsubmit="createTopic(event)">
            <div class="form-group">
                <label for="topic-name">Topic Name</label>
                <input type="text" id="topic-name" name="name" required pattern="[a-zA-Z0-9._-]+"
                       placeholder="my-topic">
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label for="partitions">Partitions</label>
                    <input type="number" id="partitions" name="partitions" value="3" min="1" max="1000">
                </div>
                <div class="form-group">
                    <label for="replication">Replication Factor</label>
                    <input type="number" id="replication" name="replication_factor" value="1" min="1" max="10">
                </div>
            </div>
            <div class="form-actions">
                <button type="button" class="btn" onclick="hideModal('create-topic-modal')">Cancel</button>
                <button type="submit" class="btn btn-primary">Create</button>
            </div>
        </form>
    </div>
</div>
"#,
            rows = rows.join("\n"),
            empty = empty_state,
        );

        self.layout("Topics", &content, "topics")
    }

    /// Render topic details page.
    pub fn topic_details(&self, topic: &TopicDetails) -> String {
        let partition_rows: Vec<String> = topic
            .partitions
            .iter()
            .map(|p| {
                format!(
                    r#"<tr>
                        <td>{id}</td>
                        <td>{leader}</td>
                        <td>{replicas}</td>
                        <td>{isr}</td>
                        <td>{start}</td>
                        <td>{end}</td>
                        <td>{size}</td>
                    </tr>"#,
                    id = p.partition_id,
                    leader = p
                        .leader
                        .map(|l| l.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                    replicas = p
                        .replicas
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    isr = p
                        .isr
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    start = p.start_offset,
                    end = p.end_offset,
                    size = format_bytes(p.size_bytes),
                )
            })
            .collect();

        let config_rows: Vec<String> = topic
            .config
            .iter()
            .map(|(k, v)| format!(r#"<tr><td>{}</td><td>{}</td></tr>"#, k, v))
            .collect();

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/topics">Topics</a> / {name}
</div>

<div class="stats-grid">
    <div class="stat-card">
        <div class="stat-label">Partitions</div>
        <div class="stat-value">{partitions}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Replication Factor</div>
        <div class="stat-value">{replication}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Messages</div>
        <div class="stat-value">{messages}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Size</div>
        <div class="stat-value">{size}</div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Partitions</h3>
    </div>
    <table class="data-table">
        <thead>
            <tr>
                <th>ID</th>
                <th>Leader</th>
                <th>Replicas</th>
                <th>ISR</th>
                <th>Start Offset</th>
                <th>End Offset</th>
                <th>Size</th>
            </tr>
        </thead>
        <tbody>
            {partition_rows}
        </tbody>
    </table>
</div>

<div class="card">
    <div class="card-header">
        <h3>Configuration</h3>
    </div>
    <table class="data-table">
        <thead>
            <tr>
                <th>Key</th>
                <th>Value</th>
            </tr>
        </thead>
        <tbody>
            {config_rows}
            {empty_config}
        </tbody>
    </table>
</div>

<div class="actions-bar">
    <a href="/topics/{name}/browse" class="btn">Browse Messages</a>
    <a href="/topics/{name}/produce" class="btn btn-primary">Produce Message</a>
    <a href="/topics/{name}/config" class="btn">Configuration</a>
    <button class="btn btn-danger" onclick="deleteTopic('{name}')">Delete Topic</button>
</div>
"#,
            name = topic.name,
            partitions = topic.partition_count,
            replication = topic.replication_factor,
            messages = format_number(topic.total_messages),
            size = format_bytes(topic.total_bytes),
            partition_rows = partition_rows.join("\n"),
            config_rows = config_rows.join("\n"),
            empty_config = if topic.config.is_empty() {
                r#"<tr><td colspan="2">
                    <div class="empty-state-container" style="padding: 24px;">
                        <div class="empty-state-desc">Using default configuration</div>
                    </div>
                </td></tr>"#
            } else {
                ""
            },
        );

        self.layout(&format!("Topic: {}", topic.name), &content, "topics")
    }

    /// Render topic message browser page.
    pub fn topic_browse(
        &self,
        topic: &TopicDetails,
        messages: Option<&ConsumeResponse>,
        current_partition: i32,
        current_offset: i64,
    ) -> String {
        let partition_options: Vec<String> = (0..topic.partition_count as i32)
            .map(|p| {
                let selected = if p == current_partition {
                    " selected"
                } else {
                    ""
                };
                format!(r#"<option value="{p}"{selected}>Partition {p}</option>"#)
            })
            .collect();

        let message_rows = if let Some(resp) = messages {
            if resp.records.is_empty() {
                r#"<div class="empty-state-container">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-title">No messages</div>
                    <div class="empty-state-desc">No messages found at this offset</div>
                </div>"#
                    .to_string()
            } else {
                resp.records
                    .iter()
                    .map(|r| {
                        let key_display = r.key.as_deref().unwrap_or("<null>");
                        let value_display = serde_json::to_string_pretty(&r.value)
                            .unwrap_or_else(|_| r.value.to_string());
                        let headers_display = if r.headers.is_empty() {
                            "None".to_string()
                        } else {
                            r.headers
                                .iter()
                                .map(|(k, v)| format!("{}: {}", k, v))
                                .collect::<Vec<_>>()
                                .join(", ")
                        };
                        let timestamp = chrono_format_timestamp(r.timestamp);
                        format!(
                            r#"<div class="message-card vim-item" data-offset="{offset}">
                                <div class="message-header">
                                    <span class="message-offset">Offset: {offset}</span>
                                    <span class="message-timestamp">{timestamp}</span>
                                    <button class="share-btn" onclick="shareMessage({offset})" title="Copy link to message">
                                        <svg viewBox="0 0 16 16" fill="currentColor"><path d="M4 4.5a2.5 2.5 0 1 1 5 0 2.5 2.5 0 0 1-5 0zm3.5 0a1 1 0 1 0-2 0 1 1 0 0 0 2 0zm4.5 6a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5zm0 1.5a1 1 0 1 0 0 2 1 1 0 0 0 0-2zm-8 0a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5zm0 1.5a1 1 0 1 0 0 2 1 1 0 0 0 0-2zm6.78-4.22l1.44 1.44-4.5 3-1.44-1.44 4.5-3z"/></svg>
                                        Share
                                    </button>
                                </div>
                                <div class="message-meta">
                                    <span><strong>Key:</strong> {key}</span>
                                    <span><strong>Headers:</strong> {headers}</span>
                                </div>
                                <div class="message-value">
                                    <pre><code>{value}</code></pre>
                                </div>
                            </div>"#,
                            offset = r.offset,
                            timestamp = timestamp,
                            key = html_escape(key_display),
                            headers = html_escape(&headers_display),
                            value = html_escape(&value_display),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        } else {
            r#"<div class="empty-state-container">
                <div class="empty-state-icon">⚠️</div>
                <div class="empty-state-title">Failed to load messages</div>
                <div class="empty-state-desc">Could not fetch messages from this partition</div>
            </div>"#
                .to_string()
        };

        let next_offset = messages.map(|m| m.next_offset).unwrap_or(current_offset);
        let prev_offset = (current_offset - 20).max(0);

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/topics">Topics</a> / <a href="/topics/{name}">{name}</a> / Browse Messages
</div>

<!-- Time Machine Timeline -->
<div class="time-machine-container card" id="time-machine">
    <div class="time-machine-header">
        <h3>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 3.5a.5.5 0 0 0-1 0V9a.5.5 0 0 0 .252.434l3.5 2a.5.5 0 0 0 .496-.868L8 8.71V3.5z"/>
                <path d="M8 16A8 8 0 1 0 8 0a8 8 0 0 0 0 16zm7-8A7 7 0 1 1 1 8a7 7 0 0 1 14 0z"/>
            </svg>
            Time Machine
        </h3>
        <div class="time-machine-controls">
            <button class="btn btn-icon" id="tm-play-btn" onclick="timeMachine.togglePlayback()" title="Play/Pause">
                <svg id="tm-play-icon" width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M10.804 8 5 4.633v6.734L10.804 8zm.792-.696a.802.802 0 0 1 0 1.392l-6.363 3.692C4.713 12.69 4 12.345 4 11.692V4.308c0-.653.713-.998 1.233-.696l6.363 3.692z"/>
                </svg>
            </button>
            <select id="tm-speed" class="tm-speed-select" onchange="timeMachine.setSpeed(this.value)">
                <option value="0.5">0.5x</option>
                <option value="1" selected>1x</option>
                <option value="2">2x</option>
                <option value="5">5x</option>
                <option value="10">10x</option>
            </select>
            <input type="datetime-local" id="tm-jump-time" class="tm-jump-input" title="Jump to timestamp">
            <button class="btn btn-small" onclick="timeMachine.jumpToTime()" title="Jump to timestamp">Go</button>
        </div>
    </div>
    <div class="time-machine-timeline">
        <div class="timeline-bar" id="timeline-bar">
            <div class="timeline-progress" id="timeline-progress"></div>
            <div class="timeline-cursor" id="timeline-cursor"></div>
            <div class="timeline-density" id="timeline-density"></div>
        </div>
        <div class="timeline-labels">
            <span id="timeline-start" class="timeline-label">--</span>
            <span id="timeline-current" class="timeline-label timeline-current">--</span>
            <span id="timeline-end" class="timeline-label">--</span>
        </div>
    </div>
    <div class="time-machine-stats">
        <span><strong>Total Messages:</strong> <span id="tm-total-messages">--</span></span>
        <span><strong>Current Offset:</strong> <span id="tm-current-offset">{current_offset}</span></span>
        <span><strong>Time Range:</strong> <span id="tm-time-range">--</span></span>
    </div>
</div>

<div class="browse-controls card">
    <form id="browse-form" method="get" action="/topics/{name}/browse">
        <div class="form-row">
            <div class="form-group">
                <label for="partition">Partition</label>
                <select name="partition" id="partition" onchange="this.form.submit()">
                    {partition_options}
                </select>
            </div>
            <div class="form-group">
                <label for="offset">Start Offset</label>
                <input type="number" name="offset" id="offset" value="{current_offset}" min="0">
            </div>
            <div class="form-group">
                <label for="limit">Messages</label>
                <input type="number" name="limit" id="limit" value="20" min="1" max="100">
            </div>
            <div class="form-group form-actions">
                <button type="submit" class="btn btn-primary">Load</button>
            </div>
        </div>
    </form>
</div>

<div class="messages-container vim-navigable">
    {message_rows}
</div>

<div class="pagination">
    <a href="/topics/{name}/browse?partition={current_partition}&offset={prev_offset}&limit=20" class="btn" {prev_disabled}>Previous</a>
    <span class="page-info">Offset: {current_offset}</span>
    <a href="/topics/{name}/browse?partition={current_partition}&offset={next_offset}&limit=20" class="btn">Next</a>
</div>
"#,
            name = topic.name,
            partition_options = partition_options.join("\n"),
            current_partition = current_partition,
            current_offset = current_offset,
            prev_offset = prev_offset,
            next_offset = next_offset,
            message_rows = message_rows,
            prev_disabled = if current_offset == 0 { "disabled" } else { "" },
        );

        self.layout(&format!("Browse: {}", topic.name), &content, "topics")
    }

    /// Render topic message producer page.
    pub fn topic_produce(&self, topic: &TopicDetails) -> String {
        let partition_options: Vec<String> = std::iter::once(
            r#"<option value="">Auto (round-robin or by key)</option>"#.to_string(),
        )
        .chain(
            (0..topic.partition_count as i32)
                .map(|p| format!(r#"<option value="{p}">Partition {p}</option>"#)),
        )
        .collect();

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/topics">Topics</a> / <a href="/topics/{name}">{name}</a> / Produce Message
</div>

<div class="card">
    <div class="card-header">
        <h3>Produce Message</h3>
        <div class="card-header-actions">
            <select id="template-selector" class="template-selector" onchange="applyTemplate(this.value)">
                <option value="">Load Template...</option>
            </select>
            <button type="button" class="btn btn-small" onclick="saveAsTemplate('{name}')">Save as Template</button>
            <button type="button" class="btn btn-small" onclick="showTemplateManager()">Manage Templates</button>
        </div>
    </div>
    <form id="produce-form" onsubmit="produceMessage(event, '{name}')">
        <div class="form-group">
            <label for="partition">Partition</label>
            <select name="partition" id="partition">
                {partition_options}
            </select>
        </div>
        <div class="form-group">
            <label for="key">Key (optional)</label>
            <input type="text" name="key" id="key" placeholder="Message key for partitioning">
            <div class="field-hint">Supports variables: <code>{{{{timestamp}}}}</code>, <code>{{{{uuid}}}}</code>, <code>{{{{random:1-100}}}}</code></div>
        </div>
        <div class="form-group">
            <label for="value">Value (JSON or plain text)</label>
            <textarea name="value" id="value" rows="10" placeholder='{{"example": "message", "id": "{{{{uuid}}}}"}}'></textarea>
            <div id="json-validation" class="validation-message"></div>
            <div class="field-hint">
                <strong>Template Variables:</strong>
                <code>{{{{timestamp}}}}</code> - Unix timestamp,
                <code>{{{{uuid}}}}</code> - Random UUID,
                <code>{{{{date}}}}</code> - ISO date,
                <code>{{{{time}}}}</code> - ISO time,
                <code>{{{{random:min-max}}}}</code> - Random number,
                <code>{{{{sequence:name}}}}</code> - Auto-incrementing sequence
            </div>
        </div>
        <div class="form-group">
            <label>Headers (optional)</label>
            <div id="headers-container">
                <div class="header-row">
                    <input type="text" name="header-key[]" placeholder="Header key">
                    <input type="text" name="header-value[]" placeholder="Header value">
                    <button type="button" class="btn btn-small" onclick="removeHeaderRow(this)">Remove</button>
                </div>
            </div>
            <button type="button" class="btn btn-small" onclick="addHeaderRow()">Add Header</button>
        </div>
        <div class="form-actions">
            <button type="submit" class="btn btn-primary">Produce Message</button>
            <a href="/topics/{name}" class="btn">Cancel</a>
        </div>
    </form>
</div>

<!-- Template Manager Modal -->
<div id="template-manager-modal" class="modal-overlay hidden">
    <div class="modal">
        <div class="modal-header">
            <h3>Message Templates</h3>
            <button type="button" class="modal-close" onclick="hideTemplateManager()">&times;</button>
        </div>
        <div class="modal-body">
            <div class="template-actions">
                <button type="button" class="btn btn-small" onclick="exportTemplates()">Export All</button>
                <label class="btn btn-small">
                    Import
                    <input type="file" accept=".json" onchange="importTemplates(event)" style="display:none">
                </label>
            </div>
            <div id="template-list" class="template-list">
                <!-- Templates will be populated here -->
            </div>
        </div>
    </div>
</div>

<div id="produce-results" class="card" style="display: none;">
    <div class="card-header">
        <h3>Recent Productions</h3>
    </div>
    <div id="results-list"></div>
</div>

<script>
// Initialize template selector on page load
document.addEventListener('DOMContentLoaded', function() {{
    updateTemplateSelector('{name}');
}});
</script>
"#,
            name = topic.name,
            partition_options = partition_options.join("\n"),
        );

        self.layout(&format!("Produce: {}", topic.name), &content, "topics")
    }

    /// Render topic configuration page.
    pub fn topic_config(&self, topic: &TopicDetails) -> String {
        // Define all known Kafka/Streamline config keys with descriptions
        let config_definitions = [
            (
                "retention.ms",
                "How long to keep messages",
                "604800000",
                "ms",
            ),
            (
                "retention.bytes",
                "Max partition size before cleanup",
                "-1",
                "bytes",
            ),
            (
                "segment.bytes",
                "Size of each log segment file",
                "1073741824",
                "bytes",
            ),
            ("cleanup.policy", "Log cleanup policy", "delete", ""),
            (
                "min.cleanable.dirty.ratio",
                "Compaction dirty ratio threshold",
                "0.5",
                "",
            ),
            (
                "delete.retention.ms",
                "Time to retain delete tombstones",
                "86400000",
                "ms",
            ),
            (
                "min.compaction.lag.ms",
                "Min time before compaction",
                "0",
                "ms",
            ),
            ("message.ttl.ms", "Message time-to-live", "0", "ms"),
            ("compression.type", "Producer compression type", "none", ""),
            (
                "max.message.bytes",
                "Maximum message size",
                "1048576",
                "bytes",
            ),
            ("min.insync.replicas", "Minimum ISR for acks=all", "1", ""),
            (
                "segment.ms",
                "Time before rolling new segment",
                "604800000",
                "ms",
            ),
            (
                "flush.messages",
                "Messages before forcing flush",
                "9223372036854775807",
                "",
            ),
            (
                "flush.ms",
                "Time before forcing flush",
                "9223372036854775807",
                "ms",
            ),
        ];

        let config_rows: Vec<String> = config_definitions
            .iter()
            .map(|(key, description, default, unit)| {
                let value = topic
                    .config
                    .get(*key)
                    .map(|v| v.as_str())
                    .unwrap_or(*default);
                let display_value = format_config_value(value, unit);
                let is_default = !topic.config.contains_key(*key);
                let badge = if is_default {
                    r#"<span class="config-badge default">Default</span>"#
                } else {
                    r#"<span class="config-badge custom">Custom</span>"#
                };
                format!(
                    r#"<tr>
                        <td class="config-key">
                            <code>{key}</code>
                            <div class="config-desc">{description}</div>
                        </td>
                        <td class="config-value">
                            <span class="config-value-text">{display_value}</span>
                            {badge}
                        </td>
                    </tr>"#,
                    key = key,
                    description = description,
                    display_value = display_value,
                    badge = badge,
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/topics">Topics</a> / <a href="/topics/{name}">{name}</a> / Configuration
</div>

<div class="config-page">
    <div class="card">
        <div class="card-header">
            <h3>Topic Configuration</h3>
            <span class="config-info">Configuration values for topic <strong>{name}</strong></span>
        </div>
        <table class="config-table">
            <thead>
                <tr>
                    <th>Configuration Key</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                {config_rows}
            </tbody>
        </table>
    </div>

    <div class="card">
        <div class="card-header">
            <h3>Configuration Presets</h3>
            <span class="config-info">Reference configurations for common use cases</span>
        </div>
        <div class="preset-grid">
            <div class="preset-card">
                <h4>High Throughput</h4>
                <p>Optimized for maximum message throughput</p>
                <ul class="preset-values">
                    <li><code>compression.type</code>: lz4</li>
                    <li><code>segment.bytes</code>: 1GB</li>
                    <li><code>flush.messages</code>: 10000</li>
                </ul>
            </div>
            <div class="preset-card">
                <h4>Low Latency</h4>
                <p>Optimized for minimal message delay</p>
                <ul class="preset-values">
                    <li><code>compression.type</code>: none</li>
                    <li><code>flush.messages</code>: 1</li>
                    <li><code>min.insync.replicas</code>: 1</li>
                </ul>
            </div>
            <div class="preset-card">
                <h4>Archival</h4>
                <p>Optimized for long-term storage</p>
                <ul class="preset-values">
                    <li><code>retention.ms</code>: -1 (forever)</li>
                    <li><code>cleanup.policy</code>: compact</li>
                    <li><code>compression.type</code>: zstd</li>
                </ul>
            </div>
            <div class="preset-card">
                <h4>Event Sourcing</h4>
                <p>Optimized for event-driven systems</p>
                <ul class="preset-values">
                    <li><code>retention.ms</code>: -1 (forever)</li>
                    <li><code>cleanup.policy</code>: compact</li>
                    <li><code>min.compaction.lag.ms</code>: 0</li>
                </ul>
            </div>
        </div>
    </div>

    <div class="form-actions">
        <a href="/topics/{name}" class="btn">Back to Topic Details</a>
    </div>
</div>
"#,
            name = topic.name,
            config_rows = config_rows.join("\n"),
        );

        self.layout(&format!("Config: {}", topic.name), &content, "topics")
    }

    /// Render consumer groups list page.
    pub fn consumer_groups_list(&self, groups: &[ConsumerGroupInfo]) -> String {
        let rows: Vec<String> = groups
            .iter()
            .map(|g| {
                let state_class = match g.state.as_str() {
                    "Stable" => "status-ok",
                    "Empty" => "status-warning",
                    "PreparingRebalance" | "CompletingRebalance" => "status-pending",
                    "Dead" => "status-error",
                    _ => "",
                };
                format!(
                    r#"<tr class="vim-item">
                        <td>
                            <button class="favorite-btn" data-type="group" data-id="{id}" onclick="toggleFavorite('group', '{id}')">☆</button>
                            <a href="/consumer-groups/{id}">{id}</a>
                        </td>
                        <td><span class="status-badge {state_class}">{state}</span></td>
                        <td>{members}</td>
                        <td>{protocol}</td>
                        <td>{coordinator}</td>
                    </tr>"#,
                    id = g.group_id,
                    state = g.state,
                    state_class = state_class,
                    members = g.member_count,
                    protocol = g.protocol_type,
                    coordinator = g
                        .coordinator
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "N/A".to_string()),
                )
            })
            .collect();

        let empty_state = if groups.is_empty() {
            r#"<tr><td colspan="5">
                <div class="empty-state-container">
                    <div class="empty-state-icon">👥</div>
                    <div class="empty-state-title">No consumer groups</div>
                    <div class="empty-state-desc">Consumer groups will appear here when clients connect and subscribe to topics</div>
                    <a href="/topics" class="btn">View Topics</a>
                </div>
            </td></tr>"#
        } else {
            ""
        };

        let content = format!(
            r#"
<div class="actions-bar">
    <div class="search-wrapper">
        <span class="search-icon">&#128269;</span>
        <input type="text" class="search-input" id="consumer-groups-search"
               placeholder="Search consumer groups..." onkeyup="filterTable('consumer-groups-table', this.value)">
        <span class="search-shortcut">/</span>
    </div>
</div>

<div class="card">
    <table class="data-table">
        <thead>
            <tr>
                <th>Group ID</th>
                <th>State</th>
                <th>Members</th>
                <th>Protocol</th>
                <th>Coordinator</th>
            </tr>
        </thead>
        <tbody id="consumer-groups-table" class="vim-navigable">
            {rows}
            {empty}
        </tbody>
    </table>
    <div class="pagination" id="consumer-groups-pagination">
        <span class="pagination-info">Showing all items</span>
        <div class="pagination-controls">
            <button class="pagination-btn prev" onclick="changePage('consumer-groups-table', -1)" disabled>← Prev</button>
            <span class="pagination-current">Page 1</span>
            <button class="pagination-btn next" onclick="changePage('consumer-groups-table', 1)">Next →</button>
            <select class="pagination-select" onchange="changePageSize('consumer-groups-table', this.value)">
                <option value="10">10 / page</option>
                <option value="25">25 / page</option>
                <option value="50">50 / page</option>
                <option value="100">100 / page</option>
            </select>
        </div>
    </div>
</div>
"#,
            rows = rows.join("\n"),
            empty = empty_state,
        );

        self.layout("Consumer Groups", &content, "consumer-groups")
    }

    /// Render consumer group details page.
    pub fn consumer_group_details(
        &self,
        group: &ConsumerGroupDetails,
        lag: Option<&ConsumerGroupLag>,
    ) -> String {
        let member_rows: Vec<String> = group
            .members
            .iter()
            .map(|m| {
                let assignments: Vec<String> = m
                    .assignments
                    .iter()
                    .map(|a| {
                        format!(
                            "{}[{}]",
                            a.topic,
                            a.partitions
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    })
                    .collect();
                format!(
                    r#"<tr>
                        <td>{member_id}</td>
                        <td>{client_id}</td>
                        <td>{host}</td>
                        <td>{assignments}</td>
                    </tr>"#,
                    member_id = m.member_id,
                    client_id = m.client_id,
                    host = m.client_host,
                    assignments = assignments.join(", "),
                )
            })
            .collect();

        let lag_rows: Vec<String> = lag
            .map(|l| {
                l.partitions
                    .iter()
                    .map(|p| {
                        let lag_class = if p.lag > 10000 {
                            "error"
                        } else if p.lag > 1000 {
                            "warning"
                        } else {
                            ""
                        };
                        format!(
                            r#"<tr>
                                <td>{topic}</td>
                                <td>{partition}</td>
                                <td>{committed}</td>
                                <td>{end}</td>
                                <td class="{lag_class}">{lag}</td>
                                <td>{consumer}</td>
                            </tr>"#,
                            topic = p.topic,
                            partition = p.partition,
                            committed = p.committed_offset,
                            end = p.log_end_offset,
                            lag = p.lag,
                            lag_class = lag_class,
                            consumer = p.consumer_id.as_deref().unwrap_or("-"),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let total_lag = lag.map(|l| l.total_lag).unwrap_or(0);
        let state_class = match group.state.as_str() {
            "Stable" => "status-ok",
            "Empty" => "status-warning",
            "PreparingRebalance" | "CompletingRebalance" => "status-pending",
            "Dead" => "status-error",
            _ => "",
        };

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/consumer-groups">Consumer Groups</a> / {group_id}
</div>

<div class="stats-grid">
    <div class="stat-card">
        <div class="stat-label">State</div>
        <div class="stat-value"><span class="status-badge {state_class}">{state}</span></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Members</div>
        <div class="stat-value">{member_count}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Total Lag</div>
        <div class="stat-value {total_lag_class}">{total_lag}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Coordinator</div>
        <div class="stat-value">{coordinator}</div>
    </div>
</div>

<div class="card" id="lag-history-card">
    <div class="card-header">
        <h3>Lag History</h3>
        <div class="lag-history-controls">
            <div class="time-range-selector">
                <button class="range-btn active" data-range="1h">1H</button>
                <button class="range-btn" data-range="6h">6H</button>
                <button class="range-btn" data-range="24h">24H</button>
                <button class="range-btn" data-range="7d">7D</button>
            </div>
            <button class="btn btn-sm btn-secondary" onclick="lagHistoryChart.exportCsv()">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                    <polyline points="7 10 12 15 17 10"/>
                    <line x1="12" y1="15" x2="12" y2="3"/>
                </svg>
                Export CSV
            </button>
        </div>
    </div>
    <div class="card-body">
        <div class="lag-velocity-indicator" id="lag-velocity">
            <span class="velocity-icon"></span>
            <span class="velocity-text">Loading...</span>
        </div>
        <div class="lag-history-chart-container" id="lag-history-chart">
            <div class="chart-loading">Loading lag history...</div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Partition Lag Sparklines</h3>
    </div>
    <div class="card-body">
        <div class="partition-sparklines" id="partition-sparklines">
            <div class="sparkline-loading">Loading partition data...</div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Members</h3>
    </div>
    <table class="data-table">
        <thead>
            <tr>
                <th>Member ID</th>
                <th>Client ID</th>
                <th>Host</th>
                <th>Assignments</th>
            </tr>
        </thead>
        <tbody>
            {member_rows}
            {empty_members}
        </tbody>
    </table>
</div>

<div class="card">
    <div class="card-header">
        <h3>Partition Lag</h3>
    </div>
    <table class="data-table">
        <thead>
            <tr>
                <th>Topic</th>
                <th>Partition</th>
                <th>Committed Offset</th>
                <th>Log End Offset</th>
                <th>Lag</th>
                <th>Consumer</th>
            </tr>
        </thead>
        <tbody>
            {lag_rows}
            {empty_lag}
        </tbody>
    </table>
</div>

<div class="actions-bar">
    <a href="/consumer-groups/{group_id}/offsets" class="btn btn-primary">Manage Offsets</a>
</div>
"#,
            group_id = group.group_id,
            state = group.state,
            state_class = state_class,
            member_count = group.members.len(),
            total_lag = total_lag,
            total_lag_class = if total_lag > 10000 {
                "error"
            } else if total_lag > 1000 {
                "warning"
            } else {
                ""
            },
            coordinator = group
                .coordinator
                .map(|c| c.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            member_rows = member_rows.join("\n"),
            empty_members = if group.members.is_empty() {
                r#"<tr><td colspan="4">
                    <div class="empty-state-container" style="padding: 24px;">
                        <div class="empty-state-icon">🔌</div>
                        <div class="empty-state-title">No active members</div>
                        <div class="empty-state-desc">Members will appear when consumers connect to this group</div>
                    </div>
                </td></tr>"#
            } else {
                ""
            },
            lag_rows = lag_rows.join("\n"),
            empty_lag = if lag.as_ref().map_or(true, |l| l.partitions.is_empty()) {
                r#"<tr><td colspan="6">
                    <div class="empty-state-container" style="padding: 24px;">
                        <div class="empty-state-icon">📊</div>
                        <div class="empty-state-title">No partition data</div>
                        <div class="empty-state-desc">Lag information will appear when the group has active subscriptions</div>
                    </div>
                </td></tr>"#
            } else {
                ""
            },
        );

        self.layout(
            &format!("Consumer Group: {}", group.group_id),
            &content,
            "consumer-groups",
        )
    }

    /// Render consumer group offset management page.
    pub fn consumer_group_offsets(
        &self,
        group: &ConsumerGroupDetails,
        lag: Option<&ConsumerGroupLag>,
        topics: &[TopicInfo],
    ) -> String {
        // Build offset table rows from lag data
        let offset_rows: Vec<String> = lag
            .map(|l| {
                l.partitions
                    .iter()
                    .map(|p| {
                        let lag_class = if p.lag > 10000 {
                            "error"
                        } else if p.lag > 1000 {
                            "warning"
                        } else {
                            ""
                        };
                        format!(
                            r#"<tr>
                                <td><input type="checkbox" class="partition-checkbox" data-topic="{topic}" data-partition="{partition}"></td>
                                <td>{topic}</td>
                                <td>{partition}</td>
                                <td>{committed}</td>
                                <td>{end}</td>
                                <td class="{lag_class}">{lag}</td>
                            </tr>"#,
                            topic = p.topic,
                            partition = p.partition,
                            committed = p.committed_offset,
                            end = p.log_end_offset,
                            lag = p.lag,
                            lag_class = lag_class,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Build topic options for dropdown
        let topic_options: Vec<String> = topics
            .iter()
            .map(|t| format!(r#"<option value="{}">{}</option>"#, t.name, t.name))
            .collect();

        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/consumer-groups">Consumer Groups</a> / <a href="/consumer-groups/{group_id}">{group_id}</a> / Offset Management
</div>

<div class="card">
    <div class="card-header">
        <h3>Reset Consumer Group Offsets</h3>
        <span class="config-info">Reset offsets for consumer group <strong>{group_id}</strong></span>
    </div>
    <form id="reset-offsets-form" onsubmit="return false;">
        <div class="form-row">
            <div class="form-group">
                <label for="reset-topic">Topic</label>
                <select id="reset-topic" required>
                    <option value="">Select a topic...</option>
                    {topic_options}
                </select>
            </div>
            <div class="form-group">
                <label for="reset-strategy">Strategy</label>
                <select id="reset-strategy" onchange="updateStrategyOptions()">
                    <option value="earliest">Earliest (beginning)</option>
                    <option value="latest">Latest (end)</option>
                    <option value="offset">Specific Offset</option>
                </select>
            </div>
        </div>
        <div class="form-group" id="specific-offset-group" style="display: none;">
            <label for="specific-offset">Offset Value</label>
            <input type="number" id="specific-offset" min="0" placeholder="Enter specific offset">
        </div>
        <div class="form-actions">
            <button type="button" class="btn" onclick="dryRunReset('{group_id}')">Preview Changes</button>
            <button type="button" class="btn btn-danger" onclick="executeReset('{group_id}')">Reset Offsets</button>
        </div>
    </form>
</div>

<div id="preview-results" class="card" style="display: none;">
    <div class="card-header">
        <h3>Preview Results</h3>
        <span class="config-info">Review changes before applying</span>
    </div>
    <div id="preview-content"></div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Current Offsets</h3>
    </div>
    <table class="data-table">
        <thead>
            <tr>
                <th style="width: 40px;"><input type="checkbox" id="select-all" onclick="toggleAllPartitions()"></th>
                <th>Topic</th>
                <th>Partition</th>
                <th>Committed Offset</th>
                <th>Log End Offset</th>
                <th>Lag</th>
            </tr>
        </thead>
        <tbody id="offsets-table">
            {offset_rows}
            {empty_offsets}
        </tbody>
    </table>
</div>

<div class="form-actions">
    <a href="/consumer-groups/{group_id}" class="btn">Back to Consumer Group</a>
</div>
"#,
            group_id = group.group_id,
            topic_options = topic_options.join("\n"),
            offset_rows = offset_rows.join("\n"),
            empty_offsets = if offset_rows.is_empty() {
                r#"<tr><td colspan="6">
                    <div class="empty-state-container" style="padding: 24px;">
                        <div class="empty-state-icon">📊</div>
                        <div class="empty-state-title">No committed offsets</div>
                        <div class="empty-state-desc">This consumer group has no committed offsets to manage</div>
                    </div>
                </td></tr>"#
            } else {
                ""
            },
        );

        self.layout(
            &format!("Offsets: {}", group.group_id),
            &content,
            "consumer-groups",
        )
    }

    /// Render brokers list page.
    pub fn brokers_list(&self, brokers: &[BrokerInfo]) -> String {
        let rows: Vec<String> = brokers
            .iter()
            .map(|b| {
                let state_class = match b.state.as_str() {
                    "Online" | "Leader" => "status-ok",
                    "Follower" => "status-ok",
                    "Offline" => "status-error",
                    _ => "",
                };
                format!(
                    r#"<tr>
                        <td>{node_id}{controller}</td>
                        <td>{host}:{port}</td>
                        <td>{rack}</td>
                        <td><span class="status-badge {state_class}">{state}</span></td>
                    </tr>"#,
                    node_id = b.node_id,
                    controller = if b.is_controller { " (Controller)" } else { "" },
                    host = b.host,
                    port = b.port,
                    rack = b.rack.as_deref().unwrap_or("-"),
                    state = b.state,
                    state_class = state_class,
                )
            })
            .collect();

        let empty_state = if brokers.is_empty() {
            r#"<tr><td colspan="4">
                <div class="empty-state-container">
                    <div class="empty-state-icon">🖥️</div>
                    <div class="empty-state-title">No brokers available</div>
                    <div class="empty-state-desc">Unable to connect to the cluster. Check that the server is running.</div>
                    <button class="btn" onclick="window.location.reload()">Refresh</button>
                </div>
            </td></tr>"#
        } else {
            ""
        };

        let content = format!(
            r#"
<div class="actions-bar">
    <div class="search-wrapper">
        <span class="search-icon">&#128269;</span>
        <input type="text" class="search-input" id="brokers-search"
               placeholder="Search brokers..." onkeyup="filterTable('brokers-table', this.value)">
        <span class="search-shortcut">/</span>
    </div>
</div>

<div class="card">
    <table class="data-table">
        <thead>
            <tr>
                <th>Node ID</th>
                <th>Address</th>
                <th>Rack</th>
                <th>State</th>
            </tr>
        </thead>
        <tbody id="brokers-table">
            {rows}
            {empty}
        </tbody>
    </table>
    <div class="pagination" id="brokers-pagination">
        <span class="pagination-info">Showing all items</span>
        <div class="pagination-controls">
            <button class="pagination-btn prev" onclick="changePage('brokers-table', -1)" disabled>← Prev</button>
            <span class="pagination-current">Page 1</span>
            <button class="pagination-btn next" onclick="changePage('brokers-table', 1)">Next →</button>
            <select class="pagination-select" onchange="changePageSize('brokers-table', this.value)">
                <option value="10">10 / page</option>
                <option value="25">25 / page</option>
                <option value="50">50 / page</option>
                <option value="100">100 / page</option>
            </select>
        </div>
    </div>
</div>
"#,
            rows = rows.join("\n"),
            empty = empty_state,
        );

        self.layout("Brokers", &content, "brokers")
    }

    /// Render metrics page.
    pub fn metrics_page(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h3>Prometheus Metrics</h3>
        <a href="/api/metrics" target="_blank" class="btn">View Raw Metrics</a>
    </div>
    <div class="card-body">
        <pre id="metrics-output" class="metrics-output">Loading metrics...</pre>
    </div>
</div>
"#;

        self.layout("Metrics", content, "metrics")
    }

    /// Render the lag heatmap page.
    pub fn lag_heatmap(&self, data: &HeatmapData) -> String {
        let empty_message = if data.groups.is_empty() || data.topics.is_empty() {
            r#"<div class="empty-state">
                <p>No consumer groups with active subscriptions found.</p>
                <p class="text-muted">The heatmap will appear when there are consumer groups subscribed to topics.</p>
            </div>"#.to_string()
        } else {
            String::new()
        };

        // Build header row (topics)
        let header_cells: Vec<String> = data
            .topics
            .iter()
            .map(|topic| {
                format!(
                    r#"<th class="heatmap-header" title="{topic}">{topic}</th>"#,
                    topic = topic
                )
            })
            .collect();
        let header_row = format!(
            r#"<tr><th class="heatmap-corner"></th>{}</tr>"#,
            header_cells.join("")
        );

        // Build data rows (groups)
        let data_rows: Vec<String> = data.groups.iter().enumerate()
            .map(|(group_idx, group)| {
                let cells: Vec<String> = data.topics.iter().enumerate()
                    .map(|(topic_idx, topic)| {
                        if let Some(Some(cell)) = data.cells.get(group_idx).and_then(|row| row.get(topic_idx)) {
                            let severity_class = match cell.severity {
                                LagSeverity::Low => "severity-low",
                                LagSeverity::Medium => "severity-medium",
                                LagSeverity::High => "severity-high",
                                LagSeverity::Critical => "severity-critical",
                            };
                            let lag_display = if cell.total_lag >= 1_000_000 {
                                format!("{:.1}M", cell.total_lag as f64 / 1_000_000.0)
                            } else if cell.total_lag >= 1_000 {
                                format!("{:.1}K", cell.total_lag as f64 / 1_000.0)
                            } else {
                                cell.total_lag.to_string()
                            };
                            format!(
                                r#"<td class="heatmap-cell {severity_class}"
                                    data-group="{group}" data-topic="{topic}"
                                    data-lag="{lag}" data-partitions="{partitions}"
                                    title="Group: {group}&#10;Topic: {topic}&#10;Lag: {lag}&#10;Partitions: {partitions}">
                                    {lag_display}
                                </td>"#,
                                severity_class = severity_class,
                                group = group,
                                topic = topic,
                                lag = cell.total_lag,
                                partitions = cell.partition_count,
                                lag_display = lag_display,
                            )
                        } else {
                            r#"<td class="heatmap-cell heatmap-empty">-</td>"#.to_string()
                        }
                    })
                    .collect();
                format!(
                    r#"<tr><th class="heatmap-row-header" title="{group}">{group}</th>{cells}</tr>"#,
                    group = group,
                    cells = cells.join("")
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="card">
    <div class="card-header">
        <h3>Consumer Lag Heatmap</h3>
        <div class="heatmap-legend">
            <span class="legend-item"><span class="legend-color severity-low"></span> Low (&lt;100)</span>
            <span class="legend-item"><span class="legend-color severity-medium"></span> Medium (100-1K)</span>
            <span class="legend-item"><span class="legend-color severity-high"></span> High (1K-10K)</span>
            <span class="legend-item"><span class="legend-color severity-critical"></span> Critical (&gt;10K)</span>
        </div>
    </div>
    <div class="card-body">
        {empty_message}
        <div class="heatmap-container">
            <table class="heatmap-table" id="heatmap-table">
                <thead>{header_row}</thead>
                <tbody>{data_rows}</tbody>
            </table>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Details</h3>
    </div>
    <div class="card-body" id="heatmap-details">
        <p class="text-muted">Click on a cell to see detailed lag information.</p>
    </div>
</div>
"#,
            empty_message = empty_message,
            header_row = header_row,
            data_rows = data_rows.join(""),
        );

        self.layout("Lag Heatmap", &content, "lag-heatmap")
    }

    /// Render the real-time log viewer page.
    pub fn logs_viewer(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h3>Server Logs</h3>
        <div class="log-controls">
            <label class="control-group">
                <span>Level:</span>
                <select id="log-level-filter" onchange="filterLogs()">
                    <option value="">All Levels</option>
                    <option value="ERROR">ERROR</option>
                    <option value="WARN">WARN</option>
                    <option value="INFO" selected>INFO</option>
                    <option value="DEBUG">DEBUG</option>
                    <option value="TRACE">TRACE</option>
                </select>
            </label>
            <label class="control-group">
                <span>Search:</span>
                <input type="text" id="log-search" placeholder="Filter logs..." oninput="filterLogs()">
            </label>
            <div class="control-group">
                <label>
                    <input type="checkbox" id="auto-scroll" checked>
                    Auto-scroll
                </label>
            </div>
            <div class="control-group">
                <label>
                    <input type="checkbox" id="log-streaming" checked onchange="toggleStreaming()">
                    Live streaming
                </label>
            </div>
            <button class="btn" onclick="clearLogDisplay()">Clear Display</button>
        </div>
    </div>
    <div class="card-body log-viewer-container">
        <div id="log-output" class="log-output">
            <div class="log-loading">Connecting to log stream...</div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Log Statistics</h3>
    </div>
    <div class="card-body">
        <div class="log-stats">
            <div class="log-stat">
                <span class="log-stat-label">Total Entries:</span>
                <span class="log-stat-value" id="stat-total">0</span>
            </div>
            <div class="log-stat">
                <span class="log-stat-label">Errors:</span>
                <span class="log-stat-value log-level-error" id="stat-errors">0</span>
            </div>
            <div class="log-stat">
                <span class="log-stat-label">Warnings:</span>
                <span class="log-stat-value log-level-warn" id="stat-warnings">0</span>
            </div>
            <div class="log-stat">
                <span class="log-stat-label">Buffer Size:</span>
                <span class="log-stat-value" id="stat-buffer">0</span>
            </div>
        </div>
    </div>
</div>
"#;

        self.layout("Server Logs", content, "logs")
    }

    /// Render the connection inspector page.
    pub fn connections(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h2>Connection Inspector</h2>
        <div class="card-actions">
            <button class="btn btn-sm" onclick="refreshConnectionStats()">Refresh</button>
        </div>
    </div>
    <div class="card-body">
        <div class="connection-stats" id="connection-stats">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="active-connections">-</div>
                    <div class="stat-label">Active Connections</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="in-flight-requests">-</div>
                    <div class="stat-label">In-flight Requests</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="server-uptime">-</div>
                    <div class="stat-label">Server Uptime</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="shutdown-status">-</div>
                    <div class="stat-label">Status</div>
                </div>
            </div>
        </div>

        <div class="connection-info" style="margin-top: 2rem;">
            <h3>Connection Information</h3>
            <div id="connection-details" class="connection-details">
                <p class="text-muted">Loading connection statistics...</p>
            </div>
        </div>
    </div>
</div>

<script>
// Initialize connection inspector when page loads
document.addEventListener('DOMContentLoaded', function() {
    initConnectionInspector();
});

function initConnectionInspector() {
    refreshConnectionStats();
    // Auto-refresh every 5 seconds
    setInterval(refreshConnectionStats, 5000);
}

function refreshConnectionStats() {
    fetch('/api/v1/connections/stats')
        .then(response => {
            if (!response.ok) throw new Error('Failed to fetch connection stats');
            return response.json();
        })
        .then(data => {
            updateConnectionDisplay(data);
        })
        .catch(error => {
            console.error('Error fetching connection stats:', error);
            document.getElementById('connection-details').innerHTML =
                `<p class="text-danger">Error loading connection statistics: ${error.message}</p>`;
        });
}

function updateConnectionDisplay(stats) {
    // Update stat cards
    document.getElementById('active-connections').textContent = stats.active_connections || 0;
    document.getElementById('in-flight-requests').textContent = stats.in_flight_requests || 0;
    document.getElementById('server-uptime').textContent = formatUptime(stats.uptime_seconds || 0);

    const statusEl = document.getElementById('shutdown-status');
    if (stats.is_shutting_down) {
        statusEl.textContent = stats.shutdown_phase || 'Shutting Down';
        statusEl.classList.add('text-warning');
        statusEl.classList.remove('text-success');
    } else {
        statusEl.textContent = 'Running';
        statusEl.classList.add('text-success');
        statusEl.classList.remove('text-warning');
    }

    // Update details section
    const detailsHtml = `
        <div class="details-table">
            <div class="detail-row">
                <span class="detail-label">Active Connections:</span>
                <span class="detail-value">${stats.active_connections || 0}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">In-flight Requests:</span>
                <span class="detail-value">${stats.in_flight_requests || 0}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Server Uptime:</span>
                <span class="detail-value">${formatUptime(stats.uptime_seconds || 0)}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Server Status:</span>
                <span class="detail-value ${stats.is_shutting_down ? 'text-warning' : 'text-success'}">
                    ${stats.is_shutting_down ? (stats.shutdown_phase || 'Shutting Down') : 'Running'}
                </span>
            </div>
            ${stats.shutdown_phase ? `
            <div class="detail-row">
                <span class="detail-label">Shutdown Phase:</span>
                <span class="detail-value">${stats.shutdown_phase}</span>
            </div>
            ` : ''}
        </div>
        <p class="text-muted" style="margin-top: 1rem; font-size: 0.875rem;">
            Note: Per-connection tracking requires protocol handler modifications.
            Statistics shown are aggregate metrics from the shutdown coordinator.
        </p>
    `;
    document.getElementById('connection-details').innerHTML = detailsHtml;
}

function formatUptime(seconds) {
    if (seconds < 60) return Math.floor(seconds) + 's';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + Math.floor(seconds % 60) + 's';
    if (seconds < 86400) {
        const hours = Math.floor(seconds / 3600);
        const mins = Math.floor((seconds % 3600) / 60);
        return hours + 'h ' + mins + 'm';
    }
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    return days + 'd ' + hours + 'h';
}
</script>
"#;

        self.layout("Connections", content, "connections")
    }

    /// Render the alerts configuration page.
    pub fn alerts(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h2>Alert Configuration</h2>
        <div class="card-actions">
            <button class="btn btn-primary btn-sm" onclick="showCreateAlertModal()">Create Alert</button>
        </div>
    </div>
    <div class="card-body">
        <!-- Alert Stats -->
        <div class="alert-stats" id="alert-stats">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="total-alerts">-</div>
                    <div class="stat-label">Total Alerts</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="enabled-alerts">-</div>
                    <div class="stat-label">Enabled</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="total-events">-</div>
                    <div class="stat-label">Events (24h)</div>
                </div>
            </div>
        </div>

        <!-- Alerts List -->
        <div class="alerts-section" style="margin-top: 2rem;">
            <h3>Configured Alerts</h3>
            <div id="alerts-list" class="alerts-list">
                <p class="text-muted">Loading alerts...</p>
            </div>
        </div>

        <!-- Alert History -->
        <div class="alert-history-section" style="margin-top: 2rem;">
            <h3>Recent Alert Events</h3>
            <div id="alert-history" class="alert-history">
                <p class="text-muted">Loading history...</p>
            </div>
        </div>
    </div>
</div>

<!-- Create/Edit Alert Modal -->
<div id="alert-modal" class="modal" style="display: none;">
    <div class="modal-backdrop" onclick="closeAlertModal()"></div>
    <div class="modal-content">
        <div class="modal-header">
            <h3 id="modal-title">Create Alert</h3>
            <button class="modal-close" onclick="closeAlertModal()">&times;</button>
        </div>
        <form id="alert-form" onsubmit="saveAlert(event)">
            <input type="hidden" id="alert-id" value="">
            <div class="form-group">
                <label for="alert-name">Name</label>
                <input type="text" id="alert-name" required placeholder="e.g., High Consumer Lag">
            </div>
            <div class="form-group">
                <label for="alert-condition">Condition Type</label>
                <select id="alert-condition" onchange="updateConditionFields()">
                    <option value="consumer_lag_exceeds">Consumer Lag Exceeds</option>
                    <option value="offline_partitions">Offline Partitions</option>
                    <option value="under_replicated_partitions">Under-replicated Partitions</option>
                    <option value="message_rate_exceeds">Message Rate Exceeds</option>
                    <option value="byte_rate_exceeds">Byte Rate Exceeds</option>
                </select>
            </div>
            <div id="condition-fields">
                <div class="form-group" id="group-field">
                    <label for="alert-group">Consumer Group</label>
                    <input type="text" id="alert-group" placeholder="* for all groups">
                </div>
                <div class="form-group" id="topic-field">
                    <label for="alert-topic">Topic</label>
                    <input type="text" id="alert-topic" placeholder="* for all topics">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label for="alert-operator">Operator</label>
                    <select id="alert-operator">
                        <option value="gt">Greater than (&gt;)</option>
                        <option value="gte">Greater or equal (&gt;=)</option>
                        <option value="lt">Less than (&lt;)</option>
                        <option value="lte">Less or equal (&lt;=)</option>
                        <option value="eq">Equals (=)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="alert-threshold">Threshold</label>
                    <input type="number" id="alert-threshold" required min="0" step="any" value="1000">
                </div>
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label for="alert-severity">Severity</label>
                    <select id="alert-severity">
                        <option value="warning">Warning</option>
                        <option value="critical">Critical</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="alert-interval">Check Interval (seconds)</label>
                    <input type="number" id="alert-interval" min="10" value="60">
                </div>
            </div>
            <div class="form-group">
                <label>Notification Channels</label>
                <div class="checkbox-group">
                    <label class="checkbox-label">
                        <input type="checkbox" id="channel-log" checked> Log to server
                    </label>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn" onclick="closeAlertModal()">Cancel</button>
                <button type="submit" class="btn btn-primary">Save Alert</button>
            </div>
        </form>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {
    initAlerts();
});

function initAlerts() {
    loadAlertStats();
    loadAlerts();
    loadAlertHistory();
    // Refresh every 30 seconds
    setInterval(function() {
        loadAlertStats();
        loadAlertHistory();
    }, 30000);
}

function loadAlertStats() {
    fetch('/api/v1/alerts/stats')
        .then(r => r.json())
        .then(stats => {
            document.getElementById('total-alerts').textContent = stats.total_alerts || 0;
            document.getElementById('enabled-alerts').textContent = stats.enabled_alerts || 0;
            document.getElementById('total-events').textContent = stats.total_events || 0;
        })
        .catch(err => console.error('Error loading alert stats:', err));
}

function loadAlerts() {
    fetch('/api/v1/alerts')
        .then(r => r.json())
        .then(data => {
            renderAlertsList(data.alerts || []);
        })
        .catch(err => {
            document.getElementById('alerts-list').innerHTML =
                '<p class="text-danger">Error loading alerts</p>';
        });
}

function renderAlertsList(alerts) {
    const container = document.getElementById('alerts-list');
    if (alerts.length === 0) {
        container.innerHTML = '<p class="text-muted">No alerts configured. Click "Create Alert" to add one.</p>';
        return;
    }

    let html = '<div class="alert-cards">';
    for (const alert of alerts) {
        const statusClass = alert.enabled ? 'enabled' : 'disabled';
        const statusText = alert.enabled ? 'Enabled' : 'Disabled';
        const severityClass = alert.severity === 'critical' ? 'severity-critical' : 'severity-warning';

        html += `
            <div class="alert-card ${statusClass}">
                <div class="alert-card-header">
                    <span class="alert-name">${escapeHtml(alert.name)}</span>
                    <span class="alert-severity ${severityClass}">${alert.severity}</span>
                </div>
                <div class="alert-card-body">
                    <div class="alert-condition">${getConditionDescription(alert.condition)}</div>
                    <div class="alert-threshold">
                        ${getOperatorSymbol(alert.operator)} ${alert.threshold}
                    </div>
                </div>
                <div class="alert-card-footer">
                    <span class="alert-status ${statusClass}">${statusText}</span>
                    <div class="alert-actions">
                        <button class="btn btn-sm" onclick="toggleAlert('${alert.id}')" title="${alert.enabled ? 'Disable' : 'Enable'}">
                            ${alert.enabled ? 'Disable' : 'Enable'}
                        </button>
                        <button class="btn btn-sm" onclick="editAlert('${alert.id}')" title="Edit">Edit</button>
                        <button class="btn btn-sm btn-danger" onclick="deleteAlert('${alert.id}')" title="Delete">Delete</button>
                    </div>
                </div>
            </div>
        `;
    }
    html += '</div>';
    container.innerHTML = html;
}

function loadAlertHistory() {
    fetch('/api/v1/alerts/history?limit=20')
        .then(r => r.json())
        .then(data => {
            renderAlertHistory(data.events || []);
        })
        .catch(err => {
            document.getElementById('alert-history').innerHTML =
                '<p class="text-danger">Error loading history</p>';
        });
}

function renderAlertHistory(events) {
    const container = document.getElementById('alert-history');
    if (events.length === 0) {
        container.innerHTML = '<p class="text-muted">No alert events yet.</p>';
        return;
    }

    let html = '<table class="table"><thead><tr><th>Time</th><th>Alert</th><th>Severity</th><th>Value</th><th>Threshold</th></tr></thead><tbody>';
    for (const event of events) {
        const time = new Date(event.timestamp).toLocaleString();
        const severityClass = event.severity === 'critical' ? 'text-danger' : 'text-warning';
        html += `
            <tr>
                <td>${time}</td>
                <td>${escapeHtml(event.alert_name)}</td>
                <td class="${severityClass}">${event.severity}</td>
                <td>${event.actual_value.toFixed(2)}</td>
                <td>${event.threshold}</td>
            </tr>
        `;
    }
    html += '</tbody></table>';
    container.innerHTML = html;
}

function showCreateAlertModal() {
    document.getElementById('modal-title').textContent = 'Create Alert';
    document.getElementById('alert-form').reset();
    document.getElementById('alert-id').value = '';
    updateConditionFields();
    document.getElementById('alert-modal').style.display = 'flex';
}

function closeAlertModal() {
    document.getElementById('alert-modal').style.display = 'none';
}

function updateConditionFields() {
    const condition = document.getElementById('alert-condition').value;
    const groupField = document.getElementById('group-field');
    const topicField = document.getElementById('topic-field');

    if (condition === 'consumer_lag_exceeds') {
        groupField.style.display = 'block';
        topicField.style.display = 'block';
    } else {
        groupField.style.display = 'none';
        topicField.style.display = 'none';
    }
}

function saveAlert(event) {
    event.preventDefault();

    const alertId = document.getElementById('alert-id').value;
    const conditionType = document.getElementById('alert-condition').value;

    let condition;
    if (conditionType === 'consumer_lag_exceeds') {
        condition = {
            type: 'consumer_lag_exceeds',
            group: document.getElementById('alert-group').value || '*',
            topic: document.getElementById('alert-topic').value || '*'
        };
    } else {
        condition = { type: conditionType };
    }

    const channels = [];
    if (document.getElementById('channel-log').checked) {
        channels.push({ type: 'log' });
    }

    const now = Date.now();
    const alertData = {
        id: alertId || crypto.randomUUID(),
        name: document.getElementById('alert-name').value,
        description: '',
        condition: condition,
        threshold: parseFloat(document.getElementById('alert-threshold').value),
        operator: document.getElementById('alert-operator').value,
        severity: document.getElementById('alert-severity').value,
        enabled: true,
        channels: channels,
        interval_secs: parseInt(document.getElementById('alert-interval').value) || 60,
        created_at: now,
        updated_at: now
    };

    const method = alertId ? 'PUT' : 'POST';
    const url = alertId ? `/api/v1/alerts/${alertId}` : '/api/v1/alerts';

    fetch(url, {
        method: method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(alertData)
    })
    .then(r => {
        if (!r.ok) throw new Error('Failed to save alert');
        return r.json();
    })
    .then(() => {
        closeAlertModal();
        loadAlerts();
        loadAlertStats();
    })
    .catch(err => {
        alert('Error saving alert: ' + err.message);
    });
}

function editAlert(id) {
    fetch(`/api/v1/alerts/${id}`)
        .then(r => r.json())
        .then(alert => {
            document.getElementById('modal-title').textContent = 'Edit Alert';
            document.getElementById('alert-id').value = alert.id;
            document.getElementById('alert-name').value = alert.name;
            document.getElementById('alert-condition').value = alert.condition.type;
            updateConditionFields();

            if (alert.condition.type === 'consumer_lag_exceeds') {
                document.getElementById('alert-group').value = alert.condition.group || '';
                document.getElementById('alert-topic').value = alert.condition.topic || '';
            }

            document.getElementById('alert-operator').value = alert.operator;
            document.getElementById('alert-threshold').value = alert.threshold;
            document.getElementById('alert-severity').value = alert.severity;
            document.getElementById('alert-interval').value = alert.interval_secs;

            document.getElementById('channel-log').checked = alert.channels.some(c => c.type === 'log');

            document.getElementById('alert-modal').style.display = 'flex';
        })
        .catch(err => alert('Error loading alert: ' + err.message));
}

function toggleAlert(id) {
    fetch(`/api/v1/alerts/${id}/toggle`, { method: 'POST' })
        .then(r => {
            if (!r.ok) throw new Error('Failed to toggle alert');
            loadAlerts();
            loadAlertStats();
        })
        .catch(err => alert('Error: ' + err.message));
}

function deleteAlert(id) {
    if (!confirm('Are you sure you want to delete this alert?')) return;

    fetch(`/api/v1/alerts/${id}`, { method: 'DELETE' })
        .then(r => {
            if (!r.ok) throw new Error('Failed to delete alert');
            loadAlerts();
            loadAlertStats();
        })
        .catch(err => alert('Error: ' + err.message));
}

function getConditionDescription(condition) {
    switch (condition.type) {
        case 'consumer_lag_exceeds':
            return `Consumer lag (${condition.group || '*'} / ${condition.topic || '*'})`;
        case 'offline_partitions':
            return 'Offline partitions count';
        case 'under_replicated_partitions':
            return 'Under-replicated partitions count';
        case 'message_rate_exceeds':
            return 'Message throughput rate';
        case 'byte_rate_exceeds':
            return 'Byte throughput rate';
        default:
            return condition.type;
    }
}

function getOperatorSymbol(op) {
    const symbols = { gt: '>', gte: '>=', lt: '<', lte: '<=', eq: '=' };
    return symbols[op] || op;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
"#;

        self.layout("Alerts", content, "alerts")
    }

    /// Render the audit log viewer page.
    pub fn audit_log(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h2>Audit Log</h2>
        <div class="card-actions">
            <button class="btn btn-small" onclick="exportAuditLog('csv')">Export CSV</button>
            <button class="btn btn-small" onclick="exportAuditLog('json')">Export JSON</button>
            <button class="btn btn-small" onclick="refreshAuditLog()">Refresh</button>
        </div>
    </div>
    <div class="card-body">
        <!-- Filters -->
        <div class="audit-filters">
            <div class="filter-group">
                <label for="event-type-filter">Event Type</label>
                <select id="event-type-filter" onchange="filterAuditLog()">
                    <option value="">All Types</option>
                    <option value="AUTH_SUCCESS">Auth Success</option>
                    <option value="AUTH_FAILURE">Auth Failure</option>
                    <option value="TOPIC_CREATE">Topic Create</option>
                    <option value="TOPIC_DELETE">Topic Delete</option>
                    <option value="ACL_ALLOW">ACL Allow</option>
                    <option value="ACL_DENY">ACL Deny</option>
                    <option value="CONFIG_CHANGE">Config Change</option>
                    <option value="CONNECTION">Connection</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="user-filter">User</label>
                <input type="text" id="user-filter" placeholder="Filter by user..." onkeyup="debounceFilterAuditLog()">
            </div>
            <div class="filter-group search-filter">
                <label for="audit-search">Search</label>
                <input type="text" id="audit-search" placeholder="Search details..." onkeyup="debounceFilterAuditLog()">
            </div>
        </div>

        <!-- Audit Log Table -->
        <table class="data-table">
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Event Type</th>
                    <th>User</th>
                    <th>Resource</th>
                    <th>Client IP</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody id="audit-log-table" class="vim-navigable">
                <tr><td colspan="6" class="text-muted">Loading audit log...</td></tr>
            </tbody>
        </table>
    </div>
</div>

<script>
let auditFilterTimeout = null;

document.addEventListener('DOMContentLoaded', function() {
    loadAuditLog();
    // Auto-refresh every 30 seconds
    setInterval(loadAuditLog, 30000);
});

function debounceFilterAuditLog() {
    clearTimeout(auditFilterTimeout);
    auditFilterTimeout = setTimeout(filterAuditLog, 300);
}

function loadAuditLog() {
    filterAuditLog();
}

function filterAuditLog() {
    const eventType = document.getElementById('event-type-filter').value;
    const user = document.getElementById('user-filter').value;
    const search = document.getElementById('audit-search').value;

    let url = '/api/v1/audit?';
    if (eventType) url += `event_type=${encodeURIComponent(eventType)}&`;
    if (user) url += `user=${encodeURIComponent(user)}&`;
    if (search) url += `search=${encodeURIComponent(search)}&`;

    fetch(url)
        .then(r => r.json())
        .then(data => {
            renderAuditLog(data.events);
        })
        .catch(err => {
            console.error('Failed to load audit log:', err);
            document.getElementById('audit-log-table').innerHTML =
                '<tr><td colspan="6" class="text-error">Failed to load audit log</td></tr>';
        });
}

function refreshAuditLog() {
    loadAuditLog();
    if (typeof StreamlineUI !== 'undefined') {
        StreamlineUI.showToast('Audit log refreshed', 'info');
    }
}

function renderAuditLog(events) {
    const tbody = document.getElementById('audit-log-table');

    if (!events || events.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-muted">No audit events found</td></tr>';
        return;
    }

    tbody.innerHTML = events.map(event => `
        <tr class="vim-item">
            <td>${escapeHtml(event.timestamp)}</td>
            <td><span class="event-type-badge ${getEventTypeClass(event.event_type)}">${escapeHtml(event.event_type)}</span></td>
            <td>${event.user ? escapeHtml(event.user) : '<span class="text-muted">-</span>'}</td>
            <td>${event.resource ? escapeHtml(event.resource) : '<span class="text-muted">-</span>'}</td>
            <td>${event.client_ip ? escapeHtml(event.client_ip) : '<span class="text-muted">-</span>'}</td>
            <td>${escapeHtml(event.details)}</td>
        </tr>
    `).join('');
}

function getEventTypeClass(eventType) {
    const typeClasses = {
        'AUTH_SUCCESS': 'event-success',
        'AUTH_FAILURE': 'event-error',
        'ACL_ALLOW': 'event-success',
        'ACL_DENY': 'event-error',
        'TOPIC_CREATE': 'event-info',
        'TOPIC_DELETE': 'event-warning',
        'CONFIG_CHANGE': 'event-warning',
        'CONNECTION': 'event-info'
    };
    return typeClasses[eventType] || 'event-default';
}

function exportAuditLog(format) {
    const eventType = document.getElementById('event-type-filter').value;
    const user = document.getElementById('user-filter').value;
    const search = document.getElementById('audit-search').value;

    let url = '/api/v1/audit?';
    if (eventType) url += `event_type=${encodeURIComponent(eventType)}&`;
    if (user) url += `user=${encodeURIComponent(user)}&`;
    if (search) url += `search=${encodeURIComponent(search)}&`;

    fetch(url)
        .then(r => r.json())
        .then(data => {
            let content, filename, mimeType;

            if (format === 'json') {
                content = JSON.stringify(data.events, null, 2);
                filename = 'audit-log.json';
                mimeType = 'application/json';
            } else {
                // CSV format
                const headers = ['Timestamp', 'Event Type', 'User', 'Resource', 'Client IP', 'Details'];
                const rows = data.events.map(e => [
                    e.timestamp,
                    e.event_type,
                    e.user || '',
                    e.resource || '',
                    e.client_ip || '',
                    e.details
                ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(','));
                content = [headers.join(','), ...rows].join('\n');
                filename = 'audit-log.csv';
                mimeType = 'text/csv';
            }

            const blob = new Blob([content], { type: mimeType });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            a.click();
            URL.revokeObjectURL(url);

            if (typeof StreamlineUI !== 'undefined') {
                StreamlineUI.showToast(`Exported audit log as ${format.toUpperCase()}`, 'success');
            }
        })
        .catch(err => {
            console.error('Export failed:', err);
            if (typeof StreamlineUI !== 'undefined') {
                StreamlineUI.showToast('Failed to export audit log', 'error');
            }
        });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
"#;

        self.layout("Audit Log", content, "audit")
    }

    /// Render DLQ dashboard page.
    pub fn dlq_dashboard(&self) -> String {
        let content = r#"
<div class="page-header">
    <h1>Dead Letter Queue Dashboard</h1>
    <div class="page-actions">
        <button class="btn" onclick="refreshDlqData()">Refresh</button>
    </div>
</div>

<!-- Stats Overview -->
<div class="dlq-stats-grid" id="dlq-stats">
    <div class="stat-card">
        <div class="stat-label">Total DLQ Topics</div>
        <div class="stat-value" id="stat-total-topics">--</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Total Messages</div>
        <div class="stat-value" id="stat-total-messages">--</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Recent (1h)</div>
        <div class="stat-value" id="stat-recent">--</div>
    </div>
    <div class="stat-card warning">
        <div class="stat-label">Spikes Detected</div>
        <div class="stat-value" id="stat-spikes">--</div>
    </div>
</div>

<!-- Error Distribution -->
<div class="card">
    <div class="card-header">
        <h3>Error Distribution</h3>
    </div>
    <div class="card-body">
        <div class="error-distribution" id="error-distribution">
            <div class="distribution-loading">Loading error distribution...</div>
        </div>
    </div>
</div>

<!-- DLQ Topics Table -->
<div class="card">
    <div class="card-header">
        <h3>DLQ Topics</h3>
        <div class="card-actions">
            <input type="text" class="search-input" id="dlq-search" placeholder="Filter topics..." onkeyup="filterDlqTopics()">
        </div>
    </div>
    <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>DLQ Topic</th>
                    <th>Source Topic</th>
                    <th>Messages</th>
                    <th>Error Types</th>
                    <th>Last Message</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="dlq-topics-table" class="vim-navigable">
                <tr><td colspan="7" class="text-muted">Loading DLQ topics...</td></tr>
            </tbody>
        </table>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', loadDlqData);

function loadDlqData() {
    fetch('/api/v1/dlq')
        .then(r => r.json())
        .then(data => {
            renderDlqStats(data.stats);
            renderErrorDistribution(data.stats.by_error_type);
            renderDlqTopics(data.topics);
        })
        .catch(err => {
            console.error('Failed to load DLQ data:', err);
            document.getElementById('dlq-topics-table').innerHTML =
                '<tr><td colspan="7" class="text-error">Failed to load DLQ data</td></tr>';
        });
}

function refreshDlqData() {
    loadDlqData();
    if (typeof StreamlineUI !== 'undefined') {
        StreamlineUI.showToast('DLQ data refreshed', 'info');
    }
}

function renderDlqStats(stats) {
    document.getElementById('stat-total-topics').textContent = stats.total_topics;
    document.getElementById('stat-total-messages').textContent = stats.total_messages.toLocaleString();
    document.getElementById('stat-recent').textContent = stats.recent_count;

    // Count spikes from topics
    fetch('/api/v1/dlq')
        .then(r => r.json())
        .then(data => {
            const spikes = data.topics.filter(t => t.has_spike).length;
            document.getElementById('stat-spikes').textContent = spikes;
        });
}

function renderErrorDistribution(byErrorType) {
    const container = document.getElementById('error-distribution');
    const total = Object.values(byErrorType).reduce((a, b) => a + b, 0);

    if (total === 0) {
        container.innerHTML = '<div class="empty-state">No errors to display</div>';
        return;
    }

    const colors = {
        'DeserializationFailure': '#ef4444',
        'ProcessingError': '#f97316',
        'SchemaMismatch': '#eab308',
        'TimeoutError': '#3b82f6',
        'Unknown': '#6b7280'
    };

    let html = '<div class="distribution-bars">';
    for (const [type, count] of Object.entries(byErrorType)) {
        const pct = ((count / total) * 100).toFixed(1);
        const color = colors[type] || '#6b7280';
        html += `
            <div class="distribution-item">
                <div class="distribution-label">
                    <span class="error-badge" style="background: ${color}20; color: ${color}">${type}</span>
                    <span class="distribution-count">${count} (${pct}%)</span>
                </div>
                <div class="distribution-bar-container">
                    <div class="distribution-bar" style="width: ${pct}%; background: ${color}"></div>
                </div>
            </div>
        `;
    }
    html += '</div>';
    container.innerHTML = html;
}

function renderDlqTopics(topics) {
    const tbody = document.getElementById('dlq-topics-table');

    if (!topics || topics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-muted">No DLQ topics found</td></tr>';
        return;
    }

    tbody.innerHTML = topics.map(topic => {
        const errorBadges = Object.entries(topic.error_counts)
            .map(([type, count]) => `<span class="error-mini-badge ${getErrorClass(type)}">${type.substring(0,3)}: ${count}</span>`)
            .join(' ');

        return `
            <tr class="vim-item" data-topic="${escapeHtml(topic.name)}">
                <td><a href="/dlq/${encodeURIComponent(topic.name)}">${escapeHtml(topic.name)}</a></td>
                <td>${topic.source_topic ? `<a href="/topics/${encodeURIComponent(topic.source_topic)}">${escapeHtml(topic.source_topic)}</a>` : '<span class="text-muted">-</span>'}</td>
                <td class="text-right">${topic.message_count.toLocaleString()}</td>
                <td>${errorBadges}</td>
                <td>${topic.last_message_time || '<span class="text-muted">-</span>'}</td>
                <td>${topic.has_spike ? '<span class="status-badge warning">Spike</span>' : '<span class="status-badge success">Normal</span>'}</td>
                <td class="actions-cell">
                    <button class="btn btn-small" onclick="retryDlq('${escapeHtml(topic.name)}')" title="Retry all messages">Retry</button>
                    <button class="btn btn-small btn-danger" onclick="purgeDlq('${escapeHtml(topic.name)}')" title="Purge all messages">Purge</button>
                </td>
            </tr>
        `;
    }).join('');
}

function getErrorClass(errorType) {
    const classes = {
        'DeserializationFailure': 'error-deser',
        'ProcessingError': 'error-proc',
        'SchemaMismatch': 'error-schema',
        'TimeoutError': 'error-timeout',
        'Unknown': 'error-unknown'
    };
    return classes[errorType] || 'error-unknown';
}

function filterDlqTopics() {
    const search = document.getElementById('dlq-search').value.toLowerCase();
    const rows = document.querySelectorAll('#dlq-topics-table tr');

    rows.forEach(row => {
        const topic = row.dataset.topic?.toLowerCase() || '';
        row.style.display = topic.includes(search) ? '' : 'none';
    });
}

function retryDlq(topic) {
    if (!confirm(`Retry all messages in ${topic}?`)) return;

    fetch(`/api/v1/dlq/${encodeURIComponent(topic)}/retry`, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                StreamlineUI.showToast(`Retrying ${data.retried_count} messages`, 'success');
                loadDlqData();
            } else {
                StreamlineUI.showToast('Retry failed', 'error');
            }
        })
        .catch(err => {
            console.error('Retry failed:', err);
            StreamlineUI.showToast('Retry failed', 'error');
        });
}

function purgeDlq(topic) {
    if (!confirm(`Permanently delete all messages in ${topic}? This cannot be undone.`)) return;

    fetch(`/api/v1/dlq/${encodeURIComponent(topic)}/purge`, { method: 'POST' })
        .then(r => r.json())
        .then(data => {
            if (data.success) {
                StreamlineUI.showToast(`Purged ${data.purged_count} messages`, 'success');
                loadDlqData();
            } else {
                StreamlineUI.showToast('Purge failed', 'error');
            }
        })
        .catch(err => {
            console.error('Purge failed:', err);
            StreamlineUI.showToast('Purge failed', 'error');
        });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
"#;

        self.layout("DLQ Dashboard", content, "dlq")
    }

    /// Render DLQ topic details page.
    pub fn dlq_topic_details(&self, topic: &str) -> String {
        let content = format!(
            r#"
<div class="breadcrumb">
    <a href="/dlq">DLQ Dashboard</a> / {topic}
</div>

<div class="page-header">
    <h1>{topic}</h1>
    <div class="page-actions">
        <button class="btn" onclick="retryAll()">Retry All</button>
        <button class="btn btn-danger" onclick="purgeAll()">Purge All</button>
    </div>
</div>

<!-- Filters -->
<div class="card dlq-filters">
    <div class="filter-row">
        <div class="filter-group">
            <label>Error Type</label>
            <select id="error-type-filter" onchange="loadMessages()">
                <option value="">All Types</option>
                <option value="DeserializationFailure">Deserialization</option>
                <option value="ProcessingError">Processing Error</option>
                <option value="SchemaMismatch">Schema Mismatch</option>
                <option value="TimeoutError">Timeout</option>
                <option value="Unknown">Unknown</option>
            </select>
        </div>
        <div class="filter-group search-filter">
            <label>Search</label>
            <input type="text" id="message-search" placeholder="Search messages..." onkeyup="filterMessages()">
        </div>
        <div class="filter-group">
            <button class="btn" onclick="loadMessages()">Refresh</button>
        </div>
    </div>
</div>

<!-- Messages -->
<div class="card">
    <div class="card-header">
        <h3>Messages (<span id="message-count">0</span>)</h3>
    </div>
    <div id="dlq-messages" class="dlq-messages-container vim-navigable">
        <div class="loading">Loading messages...</div>
    </div>
</div>

<script>
const topicName = '{topic}';

document.addEventListener('DOMContentLoaded', loadMessages);

function loadMessages() {{
    const errorType = document.getElementById('error-type-filter').value;
    let url = `/api/v1/dlq/${{encodeURIComponent(topicName)}}/messages?limit=50`;
    if (errorType) url += `&error_type=${{encodeURIComponent(errorType)}}`;

    fetch(url)
        .then(r => r.json())
        .then(data => {{
            document.getElementById('message-count').textContent = data.total;
            renderMessages(data.messages);
        }})
        .catch(err => {{
            console.error('Failed to load messages:', err);
            document.getElementById('dlq-messages').innerHTML =
                '<div class="error">Failed to load messages</div>';
        }});
}}

function renderMessages(messages) {{
    const container = document.getElementById('dlq-messages');

    if (!messages || messages.length === 0) {{
        container.innerHTML = '<div class="empty-state">No messages found</div>';
        return;
    }}

    container.innerHTML = messages.map(msg => `
        <div class="dlq-message-card vim-item" data-offset="${{msg.offset}}">
            <div class="dlq-message-header">
                <span class="dlq-offset">Offset: ${{msg.offset}}</span>
                <span class="error-badge ${{getErrorBadgeClass(msg.error_type)}}">${{msg.error_type}}</span>
                <span class="dlq-timestamp">${{msg.timestamp}}</span>
            </div>
            <div class="dlq-message-meta">
                <span><strong>Source:</strong> ${{msg.source_topic || 'Unknown'}}:${{msg.source_partition ?? '?'}}@${{msg.source_offset ?? '?'}}</span>
                <span><strong>Key:</strong> ${{msg.original_key || '<null>'}}</span>
            </div>
            <div class="dlq-error-details">
                <strong>Error:</strong> ${{escapeHtml(msg.error_details)}}
            </div>
            ${{msg.original_value_preview ? `
                <div class="dlq-message-preview">
                    <strong>Value Preview:</strong>
                    <pre><code>${{escapeHtml(msg.original_value_preview)}}</code></pre>
                </div>
            ` : ''}}
            <div class="dlq-message-actions">
                <button class="btn btn-small" onclick="retryMessage(${{msg.offset}})">Retry</button>
                <button class="btn btn-small" onclick="viewFullMessage(${{msg.offset}})">View Full</button>
            </div>
        </div>
    `).join('');
}}

function getErrorBadgeClass(errorType) {{
    const classes = {{
        'DeserializationFailure': 'error-deser',
        'ProcessingError': 'error-proc',
        'SchemaMismatch': 'error-schema',
        'TimeoutError': 'error-timeout',
        'Unknown': 'error-unknown'
    }};
    return classes[errorType] || 'error-unknown';
}}

function filterMessages() {{
    const search = document.getElementById('message-search').value.toLowerCase();
    const cards = document.querySelectorAll('.dlq-message-card');

    cards.forEach(card => {{
        const text = card.textContent.toLowerCase();
        card.style.display = text.includes(search) ? '' : 'none';
    }});
}}

function retryMessage(offset) {{
    if (!confirm(`Retry message at offset ${{offset}}?`)) return;
    StreamlineUI.showToast(`Message ${{offset}} queued for retry`, 'success');
}}

function retryAll() {{
    if (!confirm('Retry all messages in this DLQ?')) return;

    fetch(`/api/v1/dlq/${{encodeURIComponent(topicName)}}/retry`, {{ method: 'POST' }})
        .then(r => r.json())
        .then(data => {{
            if (data.success) {{
                StreamlineUI.showToast(`Retrying ${{data.retried_count}} messages`, 'success');
                loadMessages();
            }}
        }})
        .catch(err => StreamlineUI.showToast('Retry failed', 'error'));
}}

function purgeAll() {{
    if (!confirm('Permanently delete ALL messages? This cannot be undone.')) return;

    fetch(`/api/v1/dlq/${{encodeURIComponent(topicName)}}/purge`, {{ method: 'POST' }})
        .then(r => r.json())
        .then(data => {{
            if (data.success) {{
                StreamlineUI.showToast(`Purged ${{data.purged_count}} messages`, 'success');
                loadMessages();
            }}
        }})
        .catch(err => StreamlineUI.showToast('Purge failed', 'error'));
}}

function viewFullMessage(offset) {{
    // In production, this would open a modal with the full message
    alert(`View full message at offset ${{offset}} (not implemented in demo)`);
}}

function escapeHtml(text) {{
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}}
</script>
"#,
            topic = html_escape(topic)
        );

        self.layout(&format!("DLQ: {}", topic), &content, "dlq")
    }

    /// Render topic comparison page.
    pub fn topic_compare(
        &self,
        topics: &[TopicInfo],
        comparison: Option<&TopicComparison>,
        selected_topic1: Option<&str>,
        selected_topic2: Option<&str>,
    ) -> String {
        let topic1_options: String = topics
            .iter()
            .map(|t| {
                let selected = if selected_topic1 == Some(&t.name) {
                    "selected"
                } else {
                    ""
                };
                format!(
                    r#"<option value="{name}" {selected}>{name}</option>"#,
                    name = t.name,
                    selected = selected
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let topic2_options: String = topics
            .iter()
            .map(|t| {
                let selected = if selected_topic2 == Some(&t.name) {
                    "selected"
                } else {
                    ""
                };
                format!(
                    r#"<option value="{name}" {selected}>{name}</option>"#,
                    name = t.name,
                    selected = selected
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let comparison_html = if let Some(comp) = comparison {
            self.render_comparison(comp)
        } else {
            r#"<div class="empty-state-container">
                <div class="empty-state-icon">⚖️</div>
                <div class="empty-state-title">Select two topics to compare</div>
                <div class="empty-state-desc">Choose topics from the dropdowns above to see a detailed comparison.</div>
            </div>"#
                .to_string()
        };

        let content = format!(
            r#"
<div class="page-header">
    <h1>Topic Comparison</h1>
    <p class="page-desc">Compare configuration and metrics between two topics.</p>
</div>

<div class="card">
    <div class="card-header">
        <h2>Select Topics</h2>
    </div>
    <div class="card-body">
        <form id="compare-form" class="compare-selector">
            <div class="compare-topic-select">
                <label>Topic 1</label>
                <select id="topic1" name="topic1">
                    <option value="">-- Select topic --</option>
                    {topic1_options}
                </select>
            </div>
            <div class="compare-vs">VS</div>
            <div class="compare-topic-select">
                <label>Topic 2</label>
                <select id="topic2" name="topic2">
                    <option value="">-- Select topic --</option>
                    {topic2_options}
                </select>
            </div>
            <button type="submit" class="btn btn-primary">Compare</button>
        </form>
    </div>
</div>

<div id="comparison-results">
    {comparison_html}
</div>

<script>
document.getElementById('compare-form').addEventListener('submit', function(e) {{
    e.preventDefault();
    const topic1 = document.getElementById('topic1').value;
    const topic2 = document.getElementById('topic2').value;

    if (!topic1 || !topic2) {{
        alert('Please select both topics');
        return;
    }}

    if (topic1 === topic2) {{
        alert('Please select different topics');
        return;
    }}

    window.location.href = `/topics/compare?topic1=${{encodeURIComponent(topic1)}}&topic2=${{encodeURIComponent(topic2)}}`;
}});
</script>
"#,
            topic1_options = topic1_options,
            topic2_options = topic2_options,
            comparison_html = comparison_html,
        );

        self.layout("Topic Comparison", &content, "topics")
    }

    fn render_comparison(&self, comp: &TopicComparison) -> String {
        if comp.topics.len() != 2 {
            return "<p>Invalid comparison data</p>".to_string();
        }

        let t1 = &comp.topics[0];
        let t2 = &comp.topics[1];

        // Metrics comparison cards
        let metrics_html = format!(
            r#"
<div class="comparison-section">
    <h3>Metrics Overview</h3>
    <div class="comparison-metrics-grid">
        <div class="comparison-metric">
            <div class="metric-label">Partitions</div>
            <div class="metric-values">
                <span class="{p1_class}">{p1}</span>
                <span class="vs">vs</span>
                <span class="{p2_class}">{p2}</span>
            </div>
        </div>
        <div class="comparison-metric">
            <div class="metric-label">Total Messages</div>
            <div class="metric-values">
                <span class="{m1_class}">{m1}</span>
                <span class="vs">vs</span>
                <span class="{m2_class}">{m2}</span>
            </div>
        </div>
        <div class="comparison-metric">
            <div class="metric-label">Replication Factor</div>
            <div class="metric-values">
                <span>{r1}</span>
                <span class="vs">vs</span>
                <span>{r2}</span>
            </div>
        </div>
        <div class="comparison-metric">
            <div class="metric-label">Total Bytes</div>
            <div class="metric-values">
                <span>{b1}</span>
                <span class="vs">vs</span>
                <span>{b2}</span>
            </div>
        </div>
    </div>
</div>
"#,
            p1 = t1.partition_count,
            p2 = t2.partition_count,
            p1_class = if t1.partition_count > t2.partition_count {
                "leader"
            } else {
                ""
            },
            p2_class = if t2.partition_count > t1.partition_count {
                "leader"
            } else {
                ""
            },
            m1 = format_number(t1.total_messages),
            m2 = format_number(t2.total_messages),
            m1_class = if t1.total_messages > t2.total_messages {
                "leader"
            } else {
                ""
            },
            m2_class = if t2.total_messages > t1.total_messages {
                "leader"
            } else {
                ""
            },
            r1 = t1.replication_factor,
            r2 = t2.replication_factor,
            b1 = format_bytes(t1.total_bytes),
            b2 = format_bytes(t2.total_bytes),
        );

        // Config differences table
        let config_rows: Vec<String> = comp
            .config_diff
            .iter()
            .map(|diff| {
                let val1 = diff
                    .values
                    .first()
                    .and_then(|v| v.as_ref())
                    .map_or("-", |v| v);
                let val2 = diff
                    .values
                    .get(1)
                    .and_then(|v| v.as_ref())
                    .map_or("-", |v| v);
                let row_class = if diff.is_same { "" } else { "diff-highlight" };
                format!(
                    r#"<tr class="{row_class}">
                        <td>{key}</td>
                        <td>{val1}</td>
                        <td>{val2}</td>
                        <td>{status}</td>
                    </tr>"#,
                    row_class = row_class,
                    key = diff.key,
                    val1 = val1,
                    val2 = val2,
                    status = if diff.is_same {
                        r#"<span class="status-badge status-ok">Same</span>"#
                    } else {
                        r#"<span class="status-badge status-warning">Different</span>"#
                    }
                )
            })
            .collect();

        let config_html = format!(
            r#"
<div class="comparison-section">
    <h3>Configuration Comparison</h3>
    <table class="data-table comparison-table">
        <thead>
            <tr>
                <th>Config Key</th>
                <th>{t1_name}</th>
                <th>{t2_name}</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {config_rows}
        </tbody>
    </table>
</div>
"#,
            t1_name = t1.name,
            t2_name = t2.name,
            config_rows = config_rows.join("\n"),
        );

        // Topic details side-by-side
        let details_html = format!(
            r#"
<div class="comparison-section">
    <h3>Topic Details</h3>
    <div class="comparison-details-grid">
        <div class="comparison-topic-card">
            <h4>{t1_name}</h4>
            <div class="topic-stat"><span class="label">Partitions:</span> {t1_partitions}</div>
            <div class="topic-stat"><span class="label">Replication:</span> {t1_replication}</div>
            <div class="topic-stat"><span class="label">Messages:</span> {t1_messages}</div>
            <div class="topic-stat"><span class="label">Size:</span> {t1_bytes}</div>
            <div class="topic-stat"><span class="label">Internal:</span> {t1_internal}</div>
            <a href="/topics/{t1_name}" class="btn btn-sm">View Details</a>
        </div>
        <div class="comparison-topic-card">
            <h4>{t2_name}</h4>
            <div class="topic-stat"><span class="label">Partitions:</span> {t2_partitions}</div>
            <div class="topic-stat"><span class="label">Replication:</span> {t2_replication}</div>
            <div class="topic-stat"><span class="label">Messages:</span> {t2_messages}</div>
            <div class="topic-stat"><span class="label">Size:</span> {t2_bytes}</div>
            <div class="topic-stat"><span class="label">Internal:</span> {t2_internal}</div>
            <a href="/topics/{t2_name}" class="btn btn-sm">View Details</a>
        </div>
    </div>
</div>
"#,
            t1_name = t1.name,
            t2_name = t2.name,
            t1_partitions = t1.partition_count,
            t2_partitions = t2.partition_count,
            t1_replication = t1.replication_factor,
            t2_replication = t2.replication_factor,
            t1_messages = format_number(t1.total_messages),
            t2_messages = format_number(t2.total_messages),
            t1_bytes = format_bytes(t1.total_bytes),
            t2_bytes = format_bytes(t2.total_bytes),
            t1_internal = if t1.is_internal { "Yes" } else { "No" },
            t2_internal = if t2.is_internal { "Yes" } else { "No" },
        );

        format!("{}{}{}", metrics_html, config_html, details_html)
    }

    /// Render benchmark page.
    pub fn benchmark(
        &self,
        topics: &[TopicInfo],
        benchmarks: Option<&ListBenchmarksResponse>,
    ) -> String {
        let topic_options: String = topics
            .iter()
            .map(|t| format!(r#"<option value="{name}">{name}</option>"#, name = t.name))
            .collect::<Vec<_>>()
            .join("\n");

        let benchmarks_html = if let Some(resp) = benchmarks {
            if resp.benchmarks.is_empty() {
                r#"<div class="empty-state-container">
                    <div class="empty-state-icon">⏱️</div>
                    <div class="empty-state-title">No benchmarks yet</div>
                    <div class="empty-state-desc">Run a benchmark to see results here.</div>
                </div>"#
                    .to_string()
            } else {
                let rows: String = resp.benchmarks.iter().map(|b| {
                    let status_class = match &b.status {
                        BenchmarkStatus::Completed => "status-online",
                        BenchmarkStatus::Running => "status-warning",
                        BenchmarkStatus::Pending => "status-info",
                        BenchmarkStatus::Failed(_) => "status-offline",
                    };
                    let status_text = match &b.status {
                        BenchmarkStatus::Completed => "Completed",
                        BenchmarkStatus::Running => "Running",
                        BenchmarkStatus::Pending => "Pending",
                        BenchmarkStatus::Failed(_) => "Failed",
                    };
                    let bench_type = match b.benchmark_type {
                        BenchmarkType::Produce => "Produce",
                        BenchmarkType::Consume => "Consume",
                    };
                    let throughput = if let Some(ref results) = b.results {
                        format!("{:.2} msg/s", results.throughput_msg_sec)
                    } else if b.progress.elapsed_ms > 0 {
                        format!("{:.1}%", b.progress.percent_complete)
                    } else {
                        "-".to_string()
                    };
                    format!(
                        r#"<tr>
                            <td><code>{id}</code></td>
                            <td>{bench_type}</td>
                            <td>{topic}</td>
                            <td>{messages}</td>
                            <td>{msg_size}</td>
                            <td><span class="status-badge {status_class}">{status_text}</span></td>
                            <td>{throughput}</td>
                            <td>
                                <button class="btn btn-sm" onclick="viewBenchmark('{id}')">View</button>
                            </td>
                        </tr>"#,
                        id = &b.id[..8],
                        bench_type = bench_type,
                        topic = b.config.topic,
                        messages = format_number(b.config.message_count),
                        msg_size = format_bytes(b.config.message_size as u64),
                        status_class = status_class,
                        status_text = status_text,
                        throughput = throughput,
                    )
                }).collect();

                format!(
                    r#"<table class="data-table">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Type</th>
                                <th>Topic</th>
                                <th>Messages</th>
                                <th>Size</th>
                                <th>Status</th>
                                <th>Throughput</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>{rows}</tbody>
                    </table>"#,
                    rows = rows
                )
            }
        } else {
            r#"<div class="empty-state-container">
                <div class="empty-state-icon">⏱️</div>
                <div class="empty-state-title">No benchmarks yet</div>
                <div class="empty-state-desc">Run a benchmark to see results here.</div>
            </div>"#
                .to_string()
        };

        let content = format!(
            r##"
<div class="page-header">
    <h1>Benchmark Tool</h1>
    <p class="page-desc">Run performance benchmarks for produce and consume operations.</p>
</div>

<div class="benchmark-grid">
    <div class="card">
        <div class="card-header">
            <h2>Run Benchmark</h2>
        </div>
        <div class="card-body">
            <form id="benchmark-form" class="benchmark-form">
                <div class="form-group">
                    <label for="benchmark-type">Benchmark Type</label>
                    <select id="benchmark-type" name="type" required>
                        <option value="produce">Produce</option>
                        <option value="consume">Consume</option>
                    </select>
                </div>

                <div class="form-group">
                    <label for="benchmark-topic">Topic</label>
                    <select id="benchmark-topic" name="topic" required>
                        <option value="">-- Select topic --</option>
                        {topic_options}
                    </select>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label for="message-count">Message Count</label>
                        <input type="number" id="message-count" name="message_count" value="10000" min="1" max="1000000" required>
                    </div>
                    <div class="form-group">
                        <label for="message-size">Message Size (bytes)</label>
                        <input type="number" id="message-size" name="message_size" value="1024" min="1" max="1048576" required>
                    </div>
                </div>

                <div class="form-group">
                    <label for="rate-limit">Rate Limit (msg/sec, 0 = unlimited)</label>
                    <input type="number" id="rate-limit" name="rate_limit" value="0" min="0">
                </div>

                <div class="form-actions">
                    <button type="submit" class="btn btn-primary" id="start-benchmark-btn">Start Benchmark</button>
                </div>
            </form>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <h2>Active Benchmark</h2>
        </div>
        <div class="card-body">
            <div id="active-benchmark" class="active-benchmark">
                <div class="empty-state-container">
                    <div class="empty-state-icon">⏳</div>
                    <div class="empty-state-title">No active benchmark</div>
                    <div class="empty-state-desc">Start a benchmark to see progress here.</div>
                </div>
            </div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h2>Benchmark History</h2>
        <button class="btn btn-sm" onclick="refreshBenchmarks()">Refresh</button>
    </div>
    <div class="card-body">
        <div id="benchmark-history">
            {benchmarks_html}
        </div>
    </div>
</div>

<div id="benchmark-modal" class="modal" style="display:none;">
    <div class="modal-content">
        <div class="modal-header">
            <h3>Benchmark Results</h3>
            <button class="modal-close" onclick="closeModal()">&times;</button>
        </div>
        <div class="modal-body" id="benchmark-results">
        </div>
    </div>
</div>

<script>
let activeBenchmarkId = null;
let pollInterval = null;

document.getElementById('benchmark-form').addEventListener('submit', async function(e) {{
    e.preventDefault();

    const type = document.getElementById('benchmark-type').value;
    const topic = document.getElementById('benchmark-topic').value;
    const messageCount = parseInt(document.getElementById('message-count').value);
    const messageSize = parseInt(document.getElementById('message-size').value);
    const rateLimit = parseInt(document.getElementById('rate-limit').value);

    if (!topic) {{
        alert('Please select a topic');
        return;
    }}

    const config = {{
        topic: topic,
        message_count: messageCount,
        message_size: messageSize,
        rate_limit: rateLimit > 0 ? rateLimit : null
    }};

    const btn = document.getElementById('start-benchmark-btn');
    btn.disabled = true;
    btn.textContent = 'Starting...';

    try {{
        const endpoint = type === 'produce' ? '/api/v1/benchmark/produce' : '/api/v1/benchmark/consume';
        const response = await fetch(endpoint, {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(config)
        }});

        if (!response.ok) {{
            throw new Error(await response.text());
        }}

        const benchmark = await response.json();
        activeBenchmarkId = benchmark.id;
        updateActiveBenchmark(benchmark);
        startPolling();
    }} catch (err) {{
        alert('Failed to start benchmark: ' + err.message);
    }} finally {{
        btn.disabled = false;
        btn.textContent = 'Start Benchmark';
    }}
}});

function updateActiveBenchmark(benchmark) {{
    const container = document.getElementById('active-benchmark');
    const statusClass = benchmark.status.state === 'running' ? 'status-warning' :
                        benchmark.status.state === 'completed' ? 'status-online' :
                        benchmark.status.state === 'failed' ? 'status-offline' : 'status-info';

    let resultsHtml = '';
    if (benchmark.results) {{
        resultsHtml = `
            <div class="benchmark-results-grid">
                <div class="result-item">
                    <span class="result-label">Duration</span>
                    <span class="result-value">${{(benchmark.results.duration_ms / 1000).toFixed(2)}}s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Messages</span>
                    <span class="result-value">${{benchmark.results.messages_total.toLocaleString()}}</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Throughput</span>
                    <span class="result-value">${{benchmark.results.throughput_msg_sec.toFixed(2)}} msg/s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Throughput</span>
                    <span class="result-value">${{benchmark.results.throughput_mb_sec.toFixed(2)}} MB/s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">p50 Latency</span>
                    <span class="result-value">${{benchmark.results.latency_p50_ms.toFixed(2)}} ms</span>
                </div>
                <div class="result-item">
                    <span class="result-label">p99 Latency</span>
                    <span class="result-value">${{benchmark.results.latency_p99_ms.toFixed(2)}} ms</span>
                </div>
            </div>
        `;
    }}

    container.innerHTML = `
        <div class="active-benchmark-info">
            <div class="benchmark-header">
                <span class="benchmark-id"><code>${{benchmark.id.substring(0, 8)}}</code></span>
                <span class="status-badge ${{statusClass}}">${{benchmark.status.state}}</span>
            </div>
            <div class="benchmark-details">
                <span><strong>Type:</strong> ${{benchmark.benchmark_type}}</span>
                <span><strong>Topic:</strong> ${{benchmark.config.topic}}</span>
                <span><strong>Messages:</strong> ${{benchmark.config.message_count.toLocaleString()}}</span>
            </div>
            <div class="progress-container">
                <div class="progress-bar" style="width: ${{benchmark.progress.percent_complete}}%"></div>
            </div>
            <div class="progress-text">
                ${{benchmark.progress.messages_processed.toLocaleString()}} / ${{benchmark.config.message_count.toLocaleString()}} messages
                (${{benchmark.progress.percent_complete.toFixed(1)}}%)
            </div>
            ${{resultsHtml}}
        </div>
    `;
}}

function startPolling() {{
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(async () => {{
        if (!activeBenchmarkId) {{
            clearInterval(pollInterval);
            return;
        }}

        try {{
            const response = await fetch(`/api/v1/benchmark/${{activeBenchmarkId}}/status`);
            if (!response.ok) return;

            const benchmark = await response.json();
            updateActiveBenchmark(benchmark);

            if (benchmark.status.state === 'completed' || benchmark.status.state === 'failed') {{
                clearInterval(pollInterval);
                pollInterval = null;
                refreshBenchmarks();
            }}
        }} catch (err) {{
            console.error('Failed to poll benchmark status:', err);
        }}
    }}, 500);
}}

async function refreshBenchmarks() {{
    try {{
        const response = await fetch('/api/v1/benchmark');
        if (!response.ok) return;
        const data = await response.json();
        // Refresh page to show updated list
        window.location.reload();
    }} catch (err) {{
        console.error('Failed to refresh benchmarks:', err);
    }}
}}

async function viewBenchmark(id) {{
    try {{
        const response = await fetch(`/api/v1/benchmark/${{id}}/status`);
        if (!response.ok) throw new Error('Benchmark not found');

        const benchmark = await response.json();
        showBenchmarkModal(benchmark);
    }} catch (err) {{
        alert('Failed to load benchmark: ' + err.message);
    }}
}}

function showBenchmarkModal(benchmark) {{
    const modal = document.getElementById('benchmark-modal');
    const resultsDiv = document.getElementById('benchmark-results');

    let content = `
        <div class="benchmark-modal-info">
            <p><strong>ID:</strong> ${{benchmark.id}}</p>
            <p><strong>Type:</strong> ${{benchmark.benchmark_type}}</p>
            <p><strong>Topic:</strong> ${{benchmark.config.topic}}</p>
            <p><strong>Status:</strong> ${{benchmark.status.state}}</p>
        </div>
    `;

    if (benchmark.results) {{
        content += `
            <h4>Results</h4>
            <div class="benchmark-results-grid">
                <div class="result-item">
                    <span class="result-label">Duration</span>
                    <span class="result-value">${{(benchmark.results.duration_ms / 1000).toFixed(2)}}s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Messages</span>
                    <span class="result-value">${{benchmark.results.messages_total.toLocaleString()}}</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Bytes</span>
                    <span class="result-value">${{(benchmark.results.bytes_total / 1024 / 1024).toFixed(2)}} MB</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Throughput (msg)</span>
                    <span class="result-value">${{benchmark.results.throughput_msg_sec.toFixed(2)}} msg/s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Throughput (MB)</span>
                    <span class="result-value">${{benchmark.results.throughput_mb_sec.toFixed(2)}} MB/s</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Avg Latency</span>
                    <span class="result-value">${{benchmark.results.latency_avg_ms.toFixed(2)}} ms</span>
                </div>
                <div class="result-item">
                    <span class="result-label">p50 Latency</span>
                    <span class="result-value">${{benchmark.results.latency_p50_ms.toFixed(2)}} ms</span>
                </div>
                <div class="result-item">
                    <span class="result-label">p95 Latency</span>
                    <span class="result-value">${{benchmark.results.latency_p95_ms.toFixed(2)}} ms</span>
                </div>
                <div class="result-item">
                    <span class="result-label">p99 Latency</span>
                    <span class="result-value">${{benchmark.results.latency_p99_ms.toFixed(2)}} ms</span>
                </div>
                <div class="result-item">
                    <span class="result-label">Errors</span>
                    <span class="result-value">${{benchmark.results.errors}}</span>
                </div>
            </div>
        `;
    }}

    resultsDiv.innerHTML = content;
    modal.style.display = 'flex';
}}

function closeModal() {{
    document.getElementById('benchmark-modal').style.display = 'none';
}}
</script>
"##,
            topic_options = topic_options,
            benchmarks_html = benchmarks_html,
        );

        self.layout("Benchmark", &content, "benchmark")
    }

    /// Render schema list page.
    pub fn schema_list(&self) -> String {
        let content = r#"
<div class="card">
    <div class="card-header">
        <h3>Schema Registry Browser</h3>
        <div class="header-actions">
            <input type="text" id="schema-search" class="search-input" placeholder="Search schemas..." onkeyup="filterSchemas()">
            <select id="type-filter" class="filter-select" onchange="filterSchemas()">
                <option value="">All Types</option>
                <option value="avro">Avro</option>
                <option value="protobuf">Protobuf</option>
                <option value="json">JSON Schema</option>
            </select>
        </div>
    </div>
    <div class="card-content">
        <div id="schemas-loading" class="loading-spinner">Loading schemas...</div>
        <div id="schemas-list" class="schema-list" style="display: none;"></div>
        <div id="schemas-empty" class="empty-state" style="display: none;">
            <p>No schemas found</p>
        </div>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', loadSchemas);

async function loadSchemas() {
    const loading = document.getElementById('schemas-loading');
    const list = document.getElementById('schemas-list');
    const empty = document.getElementById('schemas-empty');

    try {
        const response = await fetch('/api/schemas');
        if (!response.ok) throw new Error('Failed to load schemas');
        const data = await response.json();

        if (data.subjects.length === 0) {
            loading.style.display = 'none';
            empty.style.display = 'block';
            return;
        }

        window.allSchemas = data.subjects;
        renderSchemas(data.subjects);
        loading.style.display = 'none';
        list.style.display = 'grid';
    } catch (err) {
        loading.textContent = 'Error loading schemas: ' + err.message;
    }
}

function renderSchemas(schemas) {
    const list = document.getElementById('schemas-list');
    list.innerHTML = schemas.map(s => `
        <a href="/schemas/${encodeURIComponent(s.name)}" class="schema-card" data-type="${s.schema_type}" data-name="${s.name.toLowerCase()}">
            <div class="schema-card-header">
                <span class="schema-name">${escapeHtml(s.name)}</span>
                <span class="schema-type-badge schema-type-${s.schema_type}">${s.schema_type}</span>
            </div>
            <div class="schema-card-body">
                <div class="schema-info-row">
                    <span class="schema-info-label">Latest Version</span>
                    <span class="schema-info-value">v${s.latest_version}</span>
                </div>
                <div class="schema-info-row">
                    <span class="schema-info-label">Compatibility</span>
                    <span class="compatibility-badge compat-${s.compatibility.toLowerCase()}">${s.compatibility}</span>
                </div>
                <div class="schema-info-row">
                    <span class="schema-info-label">Versions</span>
                    <span class="schema-info-value">${s.versions_count}</span>
                </div>
                <div class="schema-topics">
                    <span class="schema-info-label">Topics</span>
                    <div class="topic-tags">
                        ${s.linked_topics.map(t => `<span class="topic-tag">${escapeHtml(t)}</span>`).join('')}
                    </div>
                </div>
            </div>
        </a>
    `).join('');
}

function filterSchemas() {
    if (!window.allSchemas) return;

    const search = document.getElementById('schema-search').value.toLowerCase();
    const typeFilter = document.getElementById('type-filter').value;

    const filtered = window.allSchemas.filter(s => {
        const matchesSearch = s.name.toLowerCase().includes(search) ||
            s.linked_topics.some(t => t.toLowerCase().includes(search));
        const matchesType = !typeFilter || s.schema_type === typeFilter;
        return matchesSearch && matchesType;
    });

    renderSchemas(filtered);

    const empty = document.getElementById('schemas-empty');
    const list = document.getElementById('schemas-list');
    if (filtered.length === 0) {
        empty.style.display = 'block';
        list.style.display = 'none';
    } else {
        empty.style.display = 'none';
        list.style.display = 'grid';
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
"#;

        self.layout("Schema Registry", content, "schemas")
    }

    /// Render schema details page.
    pub fn schema_details(&self, subject: &str) -> String {
        let content = format!(
            r##"
<div class="breadcrumb">
    <a href="/schemas">Schemas</a>
    <span class="separator">/</span>
    <span>{subject}</span>
</div>

<div class="schema-header-card card">
    <div class="card-content">
        <div class="schema-header-info">
            <h2 id="subject-name">{subject}</h2>
            <div class="schema-header-badges">
                <span id="schema-type-badge" class="schema-type-badge">Loading...</span>
                <span id="compat-badge" class="compatibility-badge">Loading...</span>
            </div>
        </div>
        <div id="linked-topics" class="linked-topics"></div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Evolution Timeline</h3>
    </div>
    <div class="card-content">
        <div id="timeline-loading" class="loading-spinner">Loading version history...</div>
        <div id="timeline-container" class="evolution-timeline" style="display: none;"></div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Current Schema</h3>
        <button class="btn btn-sm" onclick="copySchema()">Copy</button>
    </div>
    <div class="card-content">
        <pre id="current-schema" class="schema-code">Loading...</pre>
    </div>
</div>

<script>
const subjectName = '{subject}';

document.addEventListener('DOMContentLoaded', loadSchemaDetails);

async function loadSchemaDetails() {{
    try {{
        const response = await fetch(`/api/schemas/${{encodeURIComponent(subjectName)}}`);
        if (!response.ok) throw new Error('Failed to load schema');
        const data = await response.json();

        window.schemaData = data;
        renderSchemaHeader(data);
        renderTimeline(data.versions);
        renderCurrentSchema(data.versions[0]);
    }} catch (err) {{
        document.getElementById('timeline-loading').textContent = 'Error: ' + err.message;
    }}
}}

function renderSchemaHeader(data) {{
    const typeBadge = document.getElementById('schema-type-badge');
    typeBadge.className = `schema-type-badge schema-type-${{data.schema_type}}`;
    typeBadge.textContent = data.schema_type;

    const compatBadge = document.getElementById('compat-badge');
    compatBadge.className = `compatibility-badge compat-${{data.compatibility.toLowerCase()}}`;
    compatBadge.textContent = data.compatibility;

    const topicsContainer = document.getElementById('linked-topics');
    topicsContainer.innerHTML = `
        <span class="topics-label">Linked Topics:</span>
        ${{data.linked_topics.map(t => `<a href="/topics/${{encodeURIComponent(t)}}" class="topic-link">${{escapeHtml(t)}}</a>`).join(', ')}}
    `;
}}

function renderTimeline(versions) {{
    const container = document.getElementById('timeline-container');
    const loading = document.getElementById('timeline-loading');

    container.innerHTML = versions.map((v, i) => `
        <div class="timeline-item ${{i === 0 ? 'current' : ''}}" data-version="${{v.version}}">
            <div class="timeline-marker">
                <div class="marker-dot"></div>
                ${{i < versions.length - 1 ? '<div class="marker-line"></div>' : ''}}
            </div>
            <div class="timeline-content" onclick="selectVersion(${{v.version}})">
                <div class="timeline-header">
                    <span class="version-number">v${{v.version}}</span>
                    <span class="version-date">${{formatDate(v.registered_at)}}</span>
                    <span class="compat-status status-${{v.compatibility_status.toLowerCase()}}">${{v.compatibility_status}}</span>
                </div>
                <div class="timeline-body">
                    ${{v.changes_summary.length > 0 ? `
                        <div class="changes-summary">
                            ${{v.changes_summary.map(c => `
                                <div class="change-item change-${{c.change_type}}">
                                    <span class="change-icon">${{getChangeIcon(c.change_type)}}</span>
                                    <span class="change-field">${{escapeHtml(c.field_name)}}</span>
                                    <span class="change-desc">${{escapeHtml(c.description)}}</span>
                                </div>
                            `).join('')}}
                        </div>
                    ` : '<div class="no-changes">Initial version</div>'}}
                </div>
                <div class="timeline-actions">
                    ${{i < versions.length - 1 ? `<a href="/schemas/${{encodeURIComponent(subjectName)}}/diff/${{versions[i + 1].version}}/${{v.version}}" class="btn btn-sm">View Diff</a>` : ''}}
                </div>
            </div>
        </div>
    `).join('');

    loading.style.display = 'none';
    container.style.display = 'block';
}}

function selectVersion(version) {{
    const v = window.schemaData.versions.find(v => v.version === version);
    if (v) {{
        renderCurrentSchema(v);
        // Update selection
        document.querySelectorAll('.timeline-item').forEach(item => {{
            item.classList.toggle('current', parseInt(item.dataset.version) === version);
        }});
    }}
}}

function renderCurrentSchema(version) {{
    const pre = document.getElementById('current-schema');
    try {{
        // Pretty print JSON
        const formatted = JSON.stringify(JSON.parse(version.schema), null, 2);
        pre.textContent = formatted;
    }} catch {{
        pre.textContent = version.schema;
    }}
}}

function copySchema() {{
    const schema = document.getElementById('current-schema').textContent;
    navigator.clipboard.writeText(schema).then(() => {{
        StreamlineUI.showToast('Schema copied to clipboard', 'success');
    }});
}}

function getChangeIcon(type) {{
    switch (type) {{
        case 'added': return '+';
        case 'removed': return '-';
        case 'modified': return '~';
        default: return '?';
    }}
}}

function formatDate(dateStr) {{
    const date = new Date(dateStr);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {{hour: '2-digit', minute:'2-digit'}});
}}

function escapeHtml(text) {{
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}}
</script>
"##,
            subject = html_escape(subject)
        );

        self.layout(&format!("Schema: {}", subject), &content, "schemas")
    }

    /// Render schema diff page.
    pub fn schema_diff(&self, subject: &str, v1: i32, v2: i32) -> String {
        let content = format!(
            r##"
<div class="breadcrumb">
    <a href="/schemas">Schemas</a>
    <span class="separator">/</span>
    <a href="/schemas/{subject}">{subject}</a>
    <span class="separator">/</span>
    <span>Diff v{v1} vs v{v2}</span>
</div>

<div class="diff-header card">
    <div class="card-content">
        <div class="diff-info">
            <h2>Schema Diff</h2>
            <div class="diff-versions">
                <span class="diff-version diff-old">v{v1}</span>
                <span class="diff-arrow">&rarr;</span>
                <span class="diff-version diff-new">v{v2}</span>
            </div>
        </div>
        <div id="compat-status" class="compatibility-status"></div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Changes Summary</h3>
    </div>
    <div class="card-content">
        <div id="changes-loading" class="loading-spinner">Loading...</div>
        <div id="changes-list" class="changes-list" style="display: none;"></div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Side-by-Side Comparison</h3>
    </div>
    <div class="card-content">
        <div class="diff-container">
            <div class="diff-panel diff-left">
                <div class="diff-panel-header">
                    <span>Version {v1}</span>
                    <button class="btn btn-sm" onclick="copyLeft()">Copy</button>
                </div>
                <pre id="schema-left" class="schema-code diff-code">Loading...</pre>
            </div>
            <div class="diff-panel diff-right">
                <div class="diff-panel-header">
                    <span>Version {v2}</span>
                    <button class="btn btn-sm" onclick="copyRight()">Copy</button>
                </div>
                <pre id="schema-right" class="schema-code diff-code">Loading...</pre>
            </div>
        </div>
    </div>
</div>

<script>
const subjectName = '{subject}';
const version1 = {v1};
const version2 = {v2};

document.addEventListener('DOMContentLoaded', loadDiff);

async function loadDiff() {{
    try {{
        const response = await fetch(`/api/schemas/${{encodeURIComponent(subjectName)}}/diff/${{version1}}/${{version2}}`);
        if (!response.ok) throw new Error('Failed to load diff');
        const data = await response.json();

        window.diffData = data;
        renderCompatStatus(data);
        renderChanges(data.changes);
        renderSchemas(data);
    }} catch (err) {{
        document.getElementById('changes-loading').textContent = 'Error: ' + err.message;
    }}
}}

function renderCompatStatus(data) {{
    const container = document.getElementById('compat-status');
    container.innerHTML = data.is_compatible
        ? '<span class="compat-badge compat-success">Compatible</span>'
        : '<span class="compat-badge compat-error">Incompatible</span>';
}}

function renderChanges(changes) {{
    const loading = document.getElementById('changes-loading');
    const list = document.getElementById('changes-list');

    if (changes.length === 0) {{
        list.innerHTML = '<p class="no-changes">No field-level changes detected</p>';
    }} else {{
        list.innerHTML = changes.map(c => `
            <div class="change-row change-${{c.change_type}}">
                <span class="change-type-badge">${{c.change_type}}</span>
                <span class="change-field-name">${{escapeHtml(c.field_name)}}</span>
                <span class="change-description">${{escapeHtml(c.description)}}</span>
                ${{c.old_value ? `<span class="change-old-value">Was: ${{escapeHtml(c.old_value)}}</span>` : ''}}
                ${{c.new_value ? `<span class="change-new-value">Now: ${{escapeHtml(c.new_value)}}</span>` : ''}}
            </div>
        `).join('');
    }}

    loading.style.display = 'none';
    list.style.display = 'block';
}}

function renderSchemas(data) {{
    const left = document.getElementById('schema-left');
    const right = document.getElementById('schema-right');

    try {{
        left.textContent = JSON.stringify(JSON.parse(data.schema1), null, 2);
    }} catch {{
        left.textContent = data.schema1;
    }}

    try {{
        right.textContent = JSON.stringify(JSON.parse(data.schema2), null, 2);
    }} catch {{
        right.textContent = data.schema2;
    }}
}}

function copyLeft() {{
    navigator.clipboard.writeText(document.getElementById('schema-left').textContent).then(() => {{
        StreamlineUI.showToast('Version {v1} schema copied', 'success');
    }});
}}

function copyRight() {{
    navigator.clipboard.writeText(document.getElementById('schema-right').textContent).then(() => {{
        StreamlineUI.showToast('Version {v2} schema copied', 'success');
    }});
}}

function escapeHtml(text) {{
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}}
</script>
"##,
            subject = html_escape(subject),
            v1 = v1,
            v2 = v2
        );

        self.layout(
            &format!("Schema Diff: {} v{} vs v{}", subject, v1, v2),
            &content,
            "schemas",
        )
    }

    /// Render clusters management page.
    pub fn clusters_page(&self) -> String {
        let content = r##"
<div class="clusters-header">
    <div class="clusters-header-left">
        <h2>Cluster Management</h2>
        <p class="subtitle">Manage multiple Streamline clusters from a single interface</p>
    </div>
    <div class="clusters-header-actions">
        <button class="btn btn-primary" onclick="showAddClusterModal()">Add Cluster</button>
        <button class="btn btn-secondary" onclick="compareAllClusters()">Compare Health</button>
    </div>
</div>

<div class="card cluster-selector-card">
    <div class="card-header">
        <h3>Current Cluster</h3>
    </div>
    <div class="card-content">
        <div id="current-cluster-display" class="current-cluster">
            <div class="current-cluster-info">
                <span class="cluster-indicator" id="current-cluster-indicator"></span>
                <span class="cluster-name" id="current-cluster-name">Loading...</span>
                <span class="cluster-url" id="current-cluster-url"></span>
            </div>
            <div class="cluster-health" id="current-cluster-health"></div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Registered Clusters</h3>
        <input type="text" id="cluster-search" class="search-input" placeholder="Search clusters..." onkeyup="filterClusters()">
    </div>
    <div class="card-content">
        <div id="clusters-loading" class="loading-spinner">Loading clusters...</div>
        <div id="clusters-grid" class="clusters-grid" style="display: none;"></div>
        <div id="clusters-empty" class="empty-state" style="display: none;">
            <p>No clusters registered</p>
            <button class="btn btn-primary" onclick="showAddClusterModal()">Add Your First Cluster</button>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3>Cross-Cluster Health Comparison</h3>
        <button class="btn btn-sm" onclick="refreshComparison()">Refresh</button>
    </div>
    <div class="card-content">
        <div id="comparison-loading" class="loading-spinner" style="display: none;">Loading comparison...</div>
        <div id="comparison-container" class="cluster-comparison"></div>
    </div>
</div>

<!-- Add/Edit Cluster Modal -->
<div id="cluster-modal" class="modal-overlay hidden">
    <div class="modal-content cluster-modal">
        <div class="modal-header">
            <h3 id="cluster-modal-title">Add Cluster</h3>
            <button class="modal-close" onclick="closeClusterModal()">&times;</button>
        </div>
        <form id="cluster-form" onsubmit="saveCluster(event)">
            <div class="form-group">
                <label for="cluster-name">Cluster Name</label>
                <input type="text" id="cluster-name" required placeholder="e.g., Production US-East">
            </div>
            <div class="form-group">
                <label for="cluster-url">Cluster URL</label>
                <input type="text" id="cluster-url" required placeholder="e.g., kafka.example.com:9092">
            </div>
            <div class="form-group">
                <label for="cluster-color">Color (optional)</label>
                <div class="color-picker">
                    <input type="color" id="cluster-color" value="#3b82f6">
                    <span class="color-preview" id="color-preview"></span>
                </div>
            </div>
            <input type="hidden" id="cluster-edit-id">
            <div class="modal-actions">
                <button type="button" class="btn btn-secondary" onclick="testConnection()">Test Connection</button>
                <button type="submit" class="btn btn-primary">Save Cluster</button>
            </div>
        </form>
        <div id="test-result" class="test-result hidden"></div>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', () => {
    loadClusters();
    loadComparison();
});

async function loadClusters() {
    const loading = document.getElementById('clusters-loading');
    const grid = document.getElementById('clusters-grid');
    const empty = document.getElementById('clusters-empty');

    try {
        const response = await fetch('/api/clusters');
        if (!response.ok) throw new Error('Failed to load clusters');
        const data = await response.json();

        window.allClusters = data.clusters;
        window.currentClusterId = data.current_cluster_id;

        if (data.clusters.length === 0) {
            loading.style.display = 'none';
            empty.style.display = 'block';
            return;
        }

        renderClusters(data.clusters);
        updateCurrentClusterDisplay(data.clusters, data.current_cluster_id);
        loading.style.display = 'none';
        grid.style.display = 'grid';
    } catch (err) {
        loading.textContent = 'Error loading clusters: ' + err.message;
    }
}

function renderClusters(clusters) {
    const grid = document.getElementById('clusters-grid');
    grid.innerHTML = clusters.map(c => `
        <div class="cluster-card ${c.is_current ? 'current' : ''}" data-id="${c.id}" data-name="${c.name.toLowerCase()}">
            <div class="cluster-card-header" style="border-left-color: ${c.color || '#3b82f6'}">
                <div class="cluster-card-title">
                    <span class="cluster-indicator" style="background: ${c.color || '#3b82f6'}"></span>
                    <span class="cluster-name">${escapeHtml(c.name)}</span>
                    ${c.is_current ? '<span class="current-badge">Current</span>' : ''}
                </div>
                <span class="health-badge health-${c.health}">${c.health}</span>
            </div>
            <div class="cluster-card-body">
                <div class="cluster-url">${escapeHtml(c.url)}</div>
                <div class="cluster-stats">
                    <div class="stat-item">
                        <span class="stat-value">${c.broker_count}</span>
                        <span class="stat-label">Brokers</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-value">${c.topic_count}</span>
                        <span class="stat-label">Topics</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-value">${formatNumber(c.message_count)}</span>
                        <span class="stat-label">Messages</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-value">${c.consumer_group_count}</span>
                        <span class="stat-label">Groups</span>
                    </div>
                </div>
                ${c.version ? `<div class="cluster-version">v${c.version}</div>` : ''}
                ${c.last_seen ? `<div class="cluster-last-seen">Last seen: ${formatTime(c.last_seen)}</div>` : ''}
            </div>
            <div class="cluster-card-actions">
                ${!c.is_current ? `<button class="btn btn-sm btn-primary" onclick="switchCluster('${c.id}')">Switch</button>` : ''}
                <button class="btn btn-sm" onclick="editCluster('${c.id}')">Edit</button>
                <button class="btn btn-sm btn-danger" onclick="deleteCluster('${c.id}', '${escapeHtml(c.name)}')">Delete</button>
            </div>
        </div>
    `).join('');
}

function updateCurrentClusterDisplay(clusters, currentId) {
    const current = clusters.find(c => c.id === currentId);
    if (!current) return;

    document.getElementById('current-cluster-indicator').style.background = current.color || '#3b82f6';
    document.getElementById('current-cluster-name').textContent = current.name;
    document.getElementById('current-cluster-url').textContent = current.url;
    document.getElementById('current-cluster-health').innerHTML = `<span class="health-badge health-${current.health}">${current.health}</span>`;
}

function filterClusters() {
    if (!window.allClusters) return;
    const search = document.getElementById('cluster-search').value.toLowerCase();
    const filtered = window.allClusters.filter(c =>
        c.name.toLowerCase().includes(search) || c.url.toLowerCase().includes(search)
    );
    renderClusters(filtered);
}

function showAddClusterModal() {
    document.getElementById('cluster-modal-title').textContent = 'Add Cluster';
    document.getElementById('cluster-form').reset();
    document.getElementById('cluster-edit-id').value = '';
    document.getElementById('test-result').classList.add('hidden');
    document.getElementById('cluster-modal').classList.remove('hidden');
}

function closeClusterModal() {
    document.getElementById('cluster-modal').classList.add('hidden');
}

function editCluster(id) {
    const cluster = window.allClusters.find(c => c.id === id);
    if (!cluster) return;

    document.getElementById('cluster-modal-title').textContent = 'Edit Cluster';
    document.getElementById('cluster-name').value = cluster.name;
    document.getElementById('cluster-url').value = cluster.url;
    document.getElementById('cluster-color').value = cluster.color || '#3b82f6';
    document.getElementById('cluster-edit-id').value = id;
    document.getElementById('test-result').classList.add('hidden');
    document.getElementById('cluster-modal').classList.remove('hidden');
}

async function saveCluster(event) {
    event.preventDefault();
    const id = document.getElementById('cluster-edit-id').value;
    const config = {
        name: document.getElementById('cluster-name').value,
        url: document.getElementById('cluster-url').value,
        color: document.getElementById('cluster-color').value
    };

    try {
        const method = id ? 'PUT' : 'POST';
        const url = id ? `/api/clusters/${id}` : '/api/clusters';
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });

        if (!response.ok) throw new Error('Failed to save cluster');

        StreamlineUI.showToast(id ? 'Cluster updated' : 'Cluster added', 'success');
        closeClusterModal();
        loadClusters();
    } catch (err) {
        StreamlineUI.showToast('Error: ' + err.message, 'error');
    }
}

async function deleteCluster(id, name) {
    if (!confirm(`Delete cluster "${name}"? This cannot be undone.`)) return;

    try {
        const response = await fetch(`/api/clusters/${id}`, { method: 'DELETE' });
        if (!response.ok) throw new Error('Failed to delete cluster');

        StreamlineUI.showToast('Cluster deleted', 'success');
        loadClusters();
    } catch (err) {
        StreamlineUI.showToast('Error: ' + err.message, 'error');
    }
}

async function switchCluster(id) {
    // In production, this would update the current cluster context
    localStorage.setItem('streamline_current_cluster', id);
    StreamlineUI.showToast('Switched to cluster', 'success');

    // Update UI to reflect switch
    window.allClusters.forEach(c => c.is_current = c.id === id);
    window.currentClusterId = id;
    renderClusters(window.allClusters);
    updateCurrentClusterDisplay(window.allClusters, id);
}

async function testConnection() {
    const url = document.getElementById('cluster-url').value;
    if (!url) {
        StreamlineUI.showToast('Please enter a cluster URL', 'error');
        return;
    }

    const resultDiv = document.getElementById('test-result');
    resultDiv.classList.remove('hidden');
    resultDiv.className = 'test-result testing';
    resultDiv.innerHTML = '<span class="spinner"></span> Testing connection...';

    try {
        // For demo, use a static test
        const id = document.getElementById('cluster-edit-id').value || 'test';
        const response = await fetch(`/api/clusters/${id}/test`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            resultDiv.className = 'test-result success';
            resultDiv.innerHTML = `
                <span class="test-icon">&#10003;</span>
                <span>Connected successfully</span>
                <span class="test-details">Latency: ${data.latency_ms}ms, Brokers: ${data.broker_count}</span>
            `;
        } else {
            throw new Error(data.message || 'Connection failed');
        }
    } catch (err) {
        resultDiv.className = 'test-result error';
        resultDiv.innerHTML = `
            <span class="test-icon">&#10007;</span>
            <span>Connection failed</span>
            <span class="test-details">${err.message}</span>
        `;
    }
}

async function loadComparison() {
    const container = document.getElementById('comparison-container');

    try {
        const response = await fetch('/api/clusters/compare');
        if (!response.ok) throw new Error('Failed to load comparison');
        const data = await response.json();

        renderComparison(data);
    } catch (err) {
        container.innerHTML = `<p class="error">Error loading comparison: ${err.message}</p>`;
    }
}

function renderComparison(data) {
    const container = document.getElementById('comparison-container');

    container.innerHTML = `
        <div class="comparison-grid">
            ${data.clusters.map(c => {
                const cluster = window.allClusters?.find(cl => cl.id === c.cluster_id) || { name: c.cluster_id, color: '#3b82f6' };
                return `
                <div class="comparison-card health-${c.health}">
                    <div class="comparison-header" style="border-left-color: ${cluster.color}">
                        <span class="comparison-name">${escapeHtml(cluster.name)}</span>
                        <span class="health-badge health-${c.health}">${c.health}</span>
                    </div>
                    <div class="comparison-body">
                        <div class="comparison-stat">
                            <span class="stat-label">Brokers</span>
                            <span class="stat-value">${c.brokers_healthy}/${c.brokers_total}</span>
                        </div>
                        <div class="comparison-stat">
                            <span class="stat-label">Under-replicated</span>
                            <span class="stat-value ${c.under_replicated_partitions > 0 ? 'warn' : ''}">${c.under_replicated_partitions}</span>
                        </div>
                        <div class="comparison-stat">
                            <span class="stat-label">Alerts</span>
                            <span class="stat-value ${c.active_alerts > 0 ? 'warn' : ''}">${c.active_alerts}</span>
                        </div>
                        ${c.error_message ? `<div class="comparison-error">${escapeHtml(c.error_message)}</div>` : ''}
                    </div>
                </div>
            `}).join('')}
        </div>
        <div class="comparison-timestamp">Last compared: ${formatTime(data.compared_at)}</div>
    `;
}

function refreshComparison() {
    const loading = document.getElementById('comparison-loading');
    loading.style.display = 'block';
    loadComparison().finally(() => {
        loading.style.display = 'none';
    });
}

function compareAllClusters() {
    refreshComparison();
    document.getElementById('comparison-container').scrollIntoView({ behavior: 'smooth' });
}

function formatNumber(n) {
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return n.toString();
}

function formatTime(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleString();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
</script>
"##;

        self.layout("Cluster Management", content, "clusters")
    }

    /// Render rebalances dashboard page.
    pub fn rebalances_page(&self) -> String {
        let content = r##"
<div class="rebalances-header">
    <h1>Partition Rebalancing</h1>
    <div class="rebalances-header-actions">
        <select id="status-filter" class="filter-select" onchange="filterRebalances()">
            <option value="">All Statuses</option>
            <option value="in_progress">In Progress</option>
            <option value="completed">Completed</option>
            <option value="stuck">Stuck</option>
            <option value="failed">Failed</option>
        </select>
        <button class="btn btn-secondary" onclick="refreshRebalances()">Refresh</button>
    </div>
</div>

<!-- Stuck Rebalances Alert -->
<div id="stuck-alert" class="rebalance-alert stuck-alert" style="display: none;">
    <div class="alert-icon">!</div>
    <div class="alert-content">
        <strong>Stuck Rebalances Detected</strong>
        <p id="stuck-count">0 consumer groups have rebalances that may be stuck.</p>
    </div>
    <button class="btn btn-warning" onclick="scrollToStuck()">View Details</button>
</div>

<!-- Recent Rebalances Timeline -->
<div class="card">
    <div class="card-header">
        <h3>Recent Rebalance Events</h3>
        <span class="rebalance-count" id="rebalance-count">0 events</span>
    </div>
    <div class="card-content">
        <div id="rebalances-loading" class="loading-spinner">Loading rebalances...</div>
        <div id="rebalances-empty" class="empty-state" style="display: none;">
            <p>No rebalance events found</p>
        </div>
        <div id="rebalances-timeline" class="rebalances-timeline" style="display: none;"></div>
    </div>
</div>

<!-- Active Rebalances Section -->
<div class="card" id="active-rebalances-card" style="display: none;">
    <div class="card-header">
        <h3>Active Rebalances</h3>
        <span class="status-badge status-in-progress">In Progress</span>
    </div>
    <div class="card-content">
        <div id="active-rebalances" class="active-rebalances-grid"></div>
    </div>
</div>

<script>
let allRebalances = [];

document.addEventListener('DOMContentLoaded', () => {
    loadRebalances();
});

async function loadRebalances() {
    const loading = document.getElementById('rebalances-loading');
    const timeline = document.getElementById('rebalances-timeline');
    const empty = document.getElementById('rebalances-empty');
    const countEl = document.getElementById('rebalance-count');

    try {
        const response = await fetch('/api/rebalances');
        if (!response.ok) throw new Error('Failed to load rebalances');
        const data = await response.json();

        allRebalances = data.events;
        countEl.textContent = `${data.total} events`;

        if (data.events.length === 0) {
            loading.style.display = 'none';
            empty.style.display = 'block';
            return;
        }

        renderRebalances(data.events);
        checkStuckRebalances(data.events);
        loading.style.display = 'none';
        timeline.style.display = 'block';
    } catch (err) {
        loading.innerHTML = `<span class="error">Error: ${err.message}</span>`;
    }
}

function renderRebalances(events) {
    const timeline = document.getElementById('rebalances-timeline');
    const activeCard = document.getElementById('active-rebalances-card');
    const activeGrid = document.getElementById('active-rebalances');

    const inProgress = events.filter(e => e.status === 'in_progress' || e.status === 'stuck');
    const completed = events.filter(e => e.status === 'completed' || e.status === 'failed');

    // Show active rebalances
    if (inProgress.length > 0) {
        activeCard.style.display = 'block';
        activeGrid.innerHTML = inProgress.map(e => renderActiveRebalance(e)).join('');
    } else {
        activeCard.style.display = 'none';
    }

    // Render timeline
    timeline.innerHTML = events.map(e => renderRebalanceEvent(e)).join('');
}

function renderActiveRebalance(e) {
    const triggerIcon = getTriggerIcon(e.trigger);
    const statusClass = e.status === 'stuck' ? 'stuck' : 'in-progress';
    const duration = e.started_at ? formatDuration(new Date() - new Date(e.started_at)) : 'Unknown';

    return `
        <div class="active-rebalance-card ${statusClass}" data-group="${e.group_id}">
            <div class="active-rebalance-header">
                <a href="/rebalances/${e.group_id}" class="group-link">${e.group_id}</a>
                <span class="rebalance-status-badge ${statusClass}">${e.status.replace('_', ' ')}</span>
            </div>
            <div class="active-rebalance-info">
                <div class="info-row">
                    <span class="label">Trigger:</span>
                    <span class="value">${triggerIcon} ${formatTrigger(e.trigger)}</span>
                </div>
                <div class="info-row">
                    <span class="label">Duration:</span>
                    <span class="value ${e.status === 'stuck' ? 'text-warning' : ''}">${duration}</span>
                </div>
                <div class="info-row">
                    <span class="label">Generation:</span>
                    <span class="value">${e.previous_generation || '-'} → ${e.generation_id}</span>
                </div>
                <div class="info-row">
                    <span class="label">Members:</span>
                    <span class="value">${e.members_before} → ${e.members_after}</span>
                </div>
            </div>
            <div class="active-rebalance-actions">
                <a href="/rebalances/${e.group_id}" class="btn btn-sm">View Details</a>
            </div>
        </div>
    `;
}

function renderRebalanceEvent(e) {
    const triggerIcon = getTriggerIcon(e.trigger);
    const statusClass = getStatusClass(e.status);
    const duration = e.duration_ms ? formatDuration(e.duration_ms) : 'In progress...';
    const time = formatTime(e.started_at);

    return `
        <div class="rebalance-event ${statusClass}" data-status="${e.status}">
            <div class="rebalance-event-marker ${statusClass}"></div>
            <div class="rebalance-event-content">
                <div class="rebalance-event-header">
                    <a href="/rebalances/${e.group_id}" class="rebalance-group-link">${e.group_id}</a>
                    <span class="rebalance-time">${time}</span>
                </div>
                <div class="rebalance-event-details">
                    <span class="rebalance-trigger">${triggerIcon} ${formatTrigger(e.trigger)}</span>
                    <span class="rebalance-status ${statusClass}">${e.status.replace('_', ' ')}</span>
                    <span class="rebalance-duration">${duration}</span>
                </div>
                <div class="rebalance-event-stats">
                    <span class="stat">
                        <span class="label">Gen:</span>
                        <span class="value">${e.previous_generation || '-'} → ${e.generation_id}</span>
                    </span>
                    <span class="stat">
                        <span class="label">Members:</span>
                        <span class="value">${e.members_before} → ${e.members_after}</span>
                    </span>
                    <span class="stat">
                        <span class="label">Partitions Moved:</span>
                        <span class="value">${e.partitions_moved}</span>
                    </span>
                </div>
            </div>
        </div>
    `;
}

function checkStuckRebalances(events) {
    const stuck = events.filter(e => e.status === 'stuck');
    const alert = document.getElementById('stuck-alert');
    const countEl = document.getElementById('stuck-count');

    if (stuck.length > 0) {
        countEl.textContent = `${stuck.length} consumer group${stuck.length > 1 ? 's have' : ' has'} rebalances that may be stuck.`;
        alert.style.display = 'flex';
    } else {
        alert.style.display = 'none';
    }
}

function filterRebalances() {
    const status = document.getElementById('status-filter').value;
    if (!status) {
        renderRebalances(allRebalances);
        return;
    }
    const filtered = allRebalances.filter(e => e.status === status);
    renderRebalances(filtered);
}

function refreshRebalances() {
    loadRebalances();
}

function scrollToStuck() {
    const stuckElement = document.querySelector('.active-rebalance-card.stuck');
    if (stuckElement) {
        stuckElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
        stuckElement.classList.add('highlight');
        setTimeout(() => stuckElement.classList.remove('highlight'), 2000);
    }
}

function getTriggerIcon(trigger) {
    const icons = {
        'member_join': '+',
        'member_leave': '-',
        'heartbeat_timeout': '!',
        'topic_metadata_change': '~',
        'manual': '*'
    };
    return icons[trigger] || '?';
}

function formatTrigger(trigger) {
    const labels = {
        'member_join': 'Member Joined',
        'member_leave': 'Member Left',
        'heartbeat_timeout': 'Heartbeat Timeout',
        'topic_metadata_change': 'Topic Metadata Change',
        'manual': 'Manual Trigger'
    };
    return labels[trigger] || trigger;
}

function getStatusClass(status) {
    const classes = {
        'in_progress': 'in-progress',
        'completed': 'completed',
        'stuck': 'stuck',
        'failed': 'failed'
    };
    return classes[status] || '';
}

function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
}

function formatTime(dateStr) {
    const date = new Date(dateStr);
    const now = new Date();
    const diff = now - date;

    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return date.toLocaleDateString();
}
</script>
"##;

        self.layout("Partition Rebalancing", content, "rebalances")
    }

    /// Render rebalance group details page.
    pub fn rebalance_group_page(&self, group_id: &str) -> String {
        let content = format!(
            r##"
<div class="rebalances-header">
    <div class="breadcrumb">
        <a href="/rebalances">Rebalances</a> / <span>{group_id}</span>
    </div>
    <h1>{group_id}</h1>
</div>

<!-- Current State -->
<div class="card">
    <div class="card-header">
        <h3>Current Assignment State</h3>
        <div class="header-actions">
            <button class="btn btn-sm btn-secondary" onclick="refreshAssignments()">Refresh</button>
        </div>
    </div>
    <div class="card-content">
        <div id="assignments-loading" class="loading-spinner">Loading assignments...</div>
        <div id="assignments-container" style="display: none;">
            <div class="assignment-summary" id="assignment-summary"></div>
            <div class="assignment-matrix" id="assignment-matrix"></div>
        </div>
    </div>
</div>

<!-- Member List -->
<div class="card">
    <div class="card-header">
        <h3>Consumer Members</h3>
        <span id="member-count" class="badge">0 members</span>
    </div>
    <div class="card-content">
        <div id="members-container" class="members-grid"></div>
    </div>
</div>

<!-- Rebalance History -->
<div class="card">
    <div class="card-header">
        <h3>Rebalance History</h3>
        <span id="history-count" class="badge">0 events</span>
    </div>
    <div class="card-content">
        <div id="history-loading" class="loading-spinner">Loading history...</div>
        <div id="history-timeline" class="rebalance-history-timeline" style="display: none;"></div>
    </div>
</div>

<!-- Assignment Diff Modal -->
<div id="diff-modal" class="modal" style="display: none;">
    <div class="modal-overlay" onclick="closeDiffModal()"></div>
    <div class="modal-content assignment-diff-modal">
        <div class="modal-header">
            <h3>Assignment Diff</h3>
            <button class="modal-close" onclick="closeDiffModal()">&times;</button>
        </div>
        <div class="modal-body" id="diff-content"></div>
    </div>
</div>

<script>
const groupId = '{group_id}';

document.addEventListener('DOMContentLoaded', () => {{
    loadAssignments();
    loadHistory();
}});

async function loadAssignments() {{
    const loading = document.getElementById('assignments-loading');
    const container = document.getElementById('assignments-container');
    const summary = document.getElementById('assignment-summary');
    const matrix = document.getElementById('assignment-matrix');
    const membersGrid = document.getElementById('members-container');
    const memberCount = document.getElementById('member-count');

    try {{
        const response = await fetch(`/api/rebalances/${{groupId}}/assignments`);
        if (!response.ok) throw new Error('Failed to load assignments');
        const data = await response.json();

        // Render summary
        summary.innerHTML = `
            <div class="summary-stat">
                <span class="value">${{data.generation_id}}</span>
                <span class="label">Generation</span>
            </div>
            <div class="summary-stat">
                <span class="value">${{data.members.length}}</span>
                <span class="label">Members</span>
            </div>
            <div class="summary-stat">
                <span class="value">${{data.total_partitions}}</span>
                <span class="label">Partitions</span>
            </div>
            <div class="summary-stat">
                <span class="value">${{data.subscribed_topics.length}}</span>
                <span class="label">Topics</span>
            </div>
        `;

        // Render assignment matrix
        renderAssignmentMatrix(data, matrix);

        // Render members
        memberCount.textContent = `${{data.members.length}} members`;
        membersGrid.innerHTML = data.members.map(m => renderMember(m)).join('');

        loading.style.display = 'none';
        container.style.display = 'block';
    }} catch (err) {{
        loading.innerHTML = `<span class="error">Error: ${{err.message}}</span>`;
    }}
}}

function renderAssignmentMatrix(data, container) {{
    // Gather all unique topics and partitions
    const topicPartitions = {{}};
    data.members.forEach(m => {{
        m.assigned_partitions.forEach(p => {{
            if (!topicPartitions[p.topic]) topicPartitions[p.topic] = new Set();
            topicPartitions[p.topic].add(p.partition);
        }});
    }});

    // Sort partitions
    Object.keys(topicPartitions).forEach(t => {{
        topicPartitions[t] = [...topicPartitions[t]].sort((a, b) => a - b);
    }});

    // Build matrix HTML
    let html = '<div class="matrix-container">';

    Object.entries(topicPartitions).forEach(([topic, partitions]) => {{
        html += `<div class="matrix-topic">
            <div class="matrix-topic-header">${{topic}}</div>
            <div class="matrix-partitions">`;

        partitions.forEach(p => {{
            const member = data.members.find(m =>
                m.assigned_partitions.some(ap => ap.topic === topic && ap.partition === p)
            );
            const color = member ? getConsumerColor(member.client_id) : '#666';
            const title = member ? `${{member.client_id}} (${{member.host}})` : 'Unassigned';

            html += `<div class="matrix-cell" style="background-color: ${{color}}" title="${{title}}">
                <span class="partition-num">${{p}}</span>
            </div>`;
        }});

        html += '</div></div>';
    }});

    html += '</div>';

    // Add legend
    html += '<div class="matrix-legend"><span class="legend-title">Consumers:</span>';
    data.members.forEach(m => {{
        const color = getConsumerColor(m.client_id);
        html += `<span class="legend-item"><span class="legend-color" style="background: ${{color}}"></span>${{m.client_id}}</span>`;
    }});
    html += '</div>';

    container.innerHTML = html;
}}

function renderMember(m) {{
    const color = getConsumerColor(m.client_id);
    const stateClass = m.state.toLowerCase().replace('_', '-');
    const lastHeartbeat = m.last_heartbeat ? formatTime(m.last_heartbeat) : 'Never';

    return `
        <div class="member-card" style="border-left-color: ${{color}}">
            <div class="member-header">
                <span class="member-client-id">${{m.client_id}}</span>
                <span class="member-state ${{stateClass}}">${{formatState(m.state)}}</span>
            </div>
            <div class="member-details">
                <div class="detail-row">
                    <span class="label">Host:</span>
                    <span class="value">${{m.host}}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Partitions:</span>
                    <span class="value">${{m.assigned_partitions.length}}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Session Timeout:</span>
                    <span class="value">${{m.session_timeout_ms}}ms</span>
                </div>
                <div class="detail-row">
                    <span class="label">Last Heartbeat:</span>
                    <span class="value">${{lastHeartbeat}}</span>
                </div>
            </div>
            <div class="member-partitions">
                ${{m.assigned_partitions.map(p => `<span class="partition-tag">${{p.topic}}:${{p.partition}}</span>`).join('')}}
            </div>
        </div>
    `;
}}

async function loadHistory() {{
    const loading = document.getElementById('history-loading');
    const timeline = document.getElementById('history-timeline');
    const countEl = document.getElementById('history-count');

    try {{
        const response = await fetch(`/api/rebalances/${{groupId}}`);
        if (!response.ok) throw new Error('Failed to load history');
        const data = await response.json();

        countEl.textContent = `${{data.total}} events`;

        if (data.events.length === 0) {{
            loading.innerHTML = '<span class="empty">No rebalance history</span>';
            return;
        }}

        timeline.innerHTML = data.events.map((e, i) => renderHistoryEvent(e, i, data.events)).join('');
        loading.style.display = 'none';
        timeline.style.display = 'block';
    }} catch (err) {{
        loading.innerHTML = `<span class="error">Error: ${{err.message}}</span>`;
    }}
}}

function renderHistoryEvent(e, index, allEvents) {{
    const statusClass = getStatusClass(e.status);
    const duration = e.duration_ms ? formatDuration(e.duration_ms) : 'In progress...';
    const time = new Date(e.started_at).toLocaleString();
    const canDiff = index < allEvents.length - 1;

    return `
        <div class="history-event ${{statusClass}}">
            <div class="history-marker ${{statusClass}}">
                <span class="gen-num">${{e.generation_id}}</span>
            </div>
            <div class="history-content">
                <div class="history-header">
                    <span class="history-time">${{time}}</span>
                    <span class="history-status ${{statusClass}}">${{e.status.replace('_', ' ')}}</span>
                </div>
                <div class="history-details">
                    <span class="trigger">${{getTriggerIcon(e.trigger)}} ${{formatTrigger(e.trigger)}}</span>
                    <span class="duration">${{duration}}</span>
                </div>
                <div class="history-stats">
                    <span>Members: ${{e.members_before}} → ${{e.members_after}}</span>
                    <span>Partitions Moved: ${{e.partitions_moved}}</span>
                </div>
                ${{canDiff ? `<button class="btn btn-sm" onclick="showDiff(${{e.previous_generation || e.generation_id - 1}}, ${{e.generation_id}})">View Diff</button>` : ''}}
            </div>
        </div>
    `;
}}

async function showDiff(gen1, gen2) {{
    const modal = document.getElementById('diff-modal');
    const content = document.getElementById('diff-content');

    content.innerHTML = '<div class="loading-spinner">Loading diff...</div>';
    modal.style.display = 'flex';

    try {{
        const response = await fetch(`/api/rebalances/${{groupId}}/diff/${{gen1}}/${{gen2}}`);
        if (!response.ok) throw new Error('Failed to load diff');
        const diff = await response.json();

        content.innerHTML = renderDiff(diff);
    }} catch (err) {{
        content.innerHTML = `<span class="error">Error: ${{err.message}}</span>`;
    }}
}}

function renderDiff(diff) {{
    let html = `<div class="diff-header">
        <span class="diff-gen">Generation ${{diff.before_generation}}</span>
        <span class="diff-arrow">→</span>
        <span class="diff-gen">Generation ${{diff.after_generation}}</span>
    </div>`;

    // Added assignments
    if (diff.added.length > 0) {{
        html += '<div class="diff-section added"><h4>Added Assignments</h4>';
        diff.added.forEach(a => {{
            html += `<div class="diff-item added">+ ${{a.topic}}:${{a.partition}} → ${{a.client_id || a.consumer_id}}</div>`;
        }});
        html += '</div>';
    }}

    // Removed assignments
    if (diff.removed.length > 0) {{
        html += '<div class="diff-section removed"><h4>Removed Assignments</h4>';
        diff.removed.forEach(r => {{
            html += `<div class="diff-item removed">- ${{r.topic}}:${{r.partition}} from ${{r.client_id || r.consumer_id}}</div>`;
        }});
        html += '</div>';
    }}

    // Moved partitions
    if (diff.moved.length > 0) {{
        html += '<div class="diff-section moved"><h4>Moved Partitions</h4>';
        diff.moved.forEach(m => {{
            html += `<div class="diff-item moved">~ ${{m.topic}}:${{m.partition}}: ${{m.from_consumer}} → ${{m.to_consumer}}</div>`;
        }});
        html += '</div>';
    }}

    if (diff.added.length === 0 && diff.removed.length === 0 && diff.moved.length === 0) {{
        html += '<div class="diff-empty">No changes in partition assignments</div>';
    }}

    return html;
}}

function closeDiffModal() {{
    document.getElementById('diff-modal').style.display = 'none';
}}

function refreshAssignments() {{
    loadAssignments();
}}

function getConsumerColor(clientId) {{
    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];
    let hash = 0;
    for (let i = 0; i < clientId.length; i++) {{
        hash = clientId.charCodeAt(i) + ((hash << 5) - hash);
    }}
    return colors[Math.abs(hash) % colors.length];
}}

function formatState(state) {{
    const labels = {{
        'stable': 'Stable',
        'preparing_rebalance': 'Preparing',
        'completing_rebalance': 'Completing',
        'dead': 'Dead'
    }};
    return labels[state] || state;
}}

function getTriggerIcon(trigger) {{
    const icons = {{
        'member_join': '+',
        'member_leave': '-',
        'heartbeat_timeout': '!',
        'topic_metadata_change': '~',
        'manual': '*'
    }};
    return icons[trigger] || '?';
}}

function formatTrigger(trigger) {{
    const labels = {{
        'member_join': 'Member Joined',
        'member_leave': 'Member Left',
        'heartbeat_timeout': 'Heartbeat Timeout',
        'topic_metadata_change': 'Topic Metadata Change',
        'manual': 'Manual Trigger'
    }};
    return labels[trigger] || trigger;
}}

function getStatusClass(status) {{
    const classes = {{
        'in_progress': 'in-progress',
        'completed': 'completed',
        'stuck': 'stuck',
        'failed': 'failed'
    }};
    return classes[status] || '';
}}

function formatDuration(ms) {{
    if (ms < 1000) return `${{ms}}ms`;
    if (ms < 60000) return `${{(ms / 1000).toFixed(1)}}s`;
    return `${{(ms / 60000).toFixed(1)}}m`;
}}

function formatTime(dateStr) {{
    const date = new Date(dateStr);
    return date.toLocaleTimeString();
}}
</script>
"##,
            group_id = group_id
        );

        self.layout(
            &format!("Rebalances - {}", group_id),
            &content,
            "rebalances",
        )
    }

    /// Render error page.
    pub fn error_page(&self, title: &str, message: &str) -> String {
        let content = format!(
            r#"
<div class="error-container">
    <h2>{title}</h2>
    <p>{message}</p>
    <a href="/" class="btn btn-primary">Go to Dashboard</a>
</div>
"#,
            title = title,
            message = message,
        );

        self.layout(title, &content, "")
    }

    /// Render a feature dashboard page (for CDC, AI, Edge, Lakehouse, Pipelines).
    pub fn feature_dashboard(
        &self,
        title: &str,
        subtitle: &str,
        description: &str,
        stats: &[(&str, &str, &str)],
        links: &[&str],
    ) -> String {
        let stats_html: String = stats
            .iter()
            .map(|(label, value, icon)| {
                format!(
                    r#"
    <div class="stat-card">
        <div class="stat-icon">{icon}</div>
        <div class="stat-label">{label}</div>
        <div class="stat-value">{value}</div>
    </div>"#,
                    icon = get_icon(icon),
                    label = label,
                    value = value,
                )
            })
            .collect();

        let links_html: String = links
            .iter()
            .map(|link| {
                let label = link.split('/').next_back().unwrap_or(link);
                let label = label.replace('-', " ");
                let label = label
                    .split_whitespace()
                    .map(|w| {
                        let mut c = w.chars();
                        match c.next() {
                            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                            None => String::new(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    r#"<a href="{link}" class="btn btn-secondary">{label}</a>"#,
                    link = link,
                    label = label,
                )
            })
            .collect::<Vec<_>>()
            .join("\n        ");

        let content = format!(
            r#"
<div class="feature-dashboard">
    <div class="feature-header">
        <h3>{subtitle}</h3>
        <p class="muted">{description}</p>
    </div>
    <div class="stats-grid">
        {stats_html}
    </div>
    <div class="feature-actions">
        {links_html}
    </div>
    <div class="feature-content" id="feature-content">
        <div class="loading-spinner">Loading...</div>
    </div>
</div>
"#,
            subtitle = subtitle,
            description = description,
            stats_html = stats_html,
            links_html = links_html,
        );

        self.layout(title, &content, &title.to_lowercase().replace(' ', "-"))
    }

    /// Render a generic list page.
    pub fn list_page(
        &self,
        title: &str,
        description: &str,
        items: &[(&str, &str, &str, &str)],
    ) -> String {
        let items_html: String = items
            .iter()
            .map(|(name, item_type, status, info)| {
                let status_class = match *status {
                    "running" | "online" | "ready" | "synced" => "success",
                    "paused" | "offline" | "pending" | "refreshing" | "syncing" => "warning",
                    "error" | "failed" => "error",
                    _ => "muted",
                };
                format!(
                    r#"
    <tr>
        <td><a href="{name}" class="link">{name}</a></td>
        <td>{item_type}</td>
        <td><span class="status-badge status-{status_class}">{status}</span></td>
        <td class="muted">{info}</td>
    </tr>"#,
                    name = html_escape(name),
                    item_type = item_type,
                    status_class = status_class,
                    status = status,
                    info = info,
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="list-page">
    <div class="page-header">
        <p class="muted">{description}</p>
    </div>
    <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Info</th>
                </tr>
            </thead>
            <tbody>
                {items_html}
            </tbody>
        </table>
    </div>
</div>
"#,
            description = description,
            items_html = items_html,
        );

        self.layout(title, &content, "")
    }

    /// Render a generic details page.
    pub fn details_page(&self, title: &str, details: &[(&str, &str)]) -> String {
        let details_html: String = details
            .iter()
            .map(|(label, value)| {
                format!(
                    r#"
    <div class="detail-row">
        <span class="detail-label">{label}</span>
        <span class="detail-value">{value}</span>
    </div>"#,
                    label = label,
                    value = html_escape(value),
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="details-page">
    <div class="details-card">
        {details_html}
    </div>
    <div class="details-actions">
        <button class="btn btn-secondary" onclick="history.back()">Back</button>
    </div>
</div>
"#,
            details_html = details_html,
        );

        self.layout(title, &content, "")
    }

    /// Render the semantic search page for AI.
    pub fn search_page(&self, title: &str, description: &str, topics: &[&str]) -> String {
        let topic_options: String = topics
            .iter()
            .map(|t| format!(r#"<option value="{t}">{t}</option>"#, t = t))
            .collect::<Vec<_>>()
            .join("\n                ");

        let content = format!(
            r#"
<div class="search-page">
    <div class="page-header">
        <p class="muted">{description}</p>
    </div>
    <div class="search-form">
        <div class="form-group">
            <label for="search-query">Search Query</label>
            <input type="text" id="search-query" class="form-input" placeholder="Enter your search query...">
        </div>
        <div class="form-group">
            <label for="search-topics">Topics to Search</label>
            <select id="search-topics" class="form-select" multiple>
                {topic_options}
            </select>
        </div>
        <div class="form-group">
            <label for="search-limit">Results Limit</label>
            <input type="number" id="search-limit" class="form-input" value="10" min="1" max="100">
        </div>
        <button class="btn btn-primary" onclick="performSemanticSearch()">Search</button>
    </div>
    <div class="search-results" id="search-results">
        <p class="muted">Enter a query and click Search to find semantically similar messages.</p>
    </div>
</div>
<script>
async function performSemanticSearch() {{
    const query = document.getElementById('search-query').value;
    const topicsSelect = document.getElementById('search-topics');
    const topics = Array.from(topicsSelect.selectedOptions).map(o => o.value);
    const limit = parseInt(document.getElementById('search-limit').value) || 10;

    const resultsDiv = document.getElementById('search-results');
    resultsDiv.innerHTML = '<div class="loading-spinner">Searching...</div>';

    try {{
        const response = await fetch('/api/ai/search', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ query, topics, limit }})
        }});
        const data = await response.json();

        if (data.results && data.results.length > 0) {{
            resultsDiv.innerHTML = data.results.map(r => `
                <div class="search-result">
                    <div class="result-header">
                        <span class="result-topic">${{r.topic}}</span>
                        <span class="result-offset">Offset: ${{r.offset}}</span>
                        <span class="result-similarity">${{(r.similarity * 100).toFixed(1)}}% match</span>
                    </div>
                    <div class="result-content">${{r.content}}</div>
                </div>
            `).join('');
        }} else {{
            resultsDiv.innerHTML = '<p class="muted">No results found.</p>';
        }}
    }} catch (e) {{
        resultsDiv.innerHTML = '<p class="error">Search failed: ' + e.message + '</p>';
    }}
}}
</script>
"#,
            description = description,
            topic_options = topic_options,
        );

        self.layout(title, &content, "ai")
    }

    /// Render the anomalies page for AI.
    pub fn anomalies_page(&self, title: &str, anomalies: &[(&str, &str, &str, &str)]) -> String {
        let anomalies_html: String = anomalies
            .iter()
            .map(|(severity, description, time, topic)| {
                let severity_class = match *severity {
                    "HIGH" => "error",
                    "MEDIUM" => "warning",
                    "LOW" => "info",
                    _ => "muted",
                };
                format!(
                    r#"
    <div class="anomaly-item anomaly-{severity_class}">
        <div class="anomaly-header">
            <span class="anomaly-severity badge badge-{severity_class}">{severity}</span>
            <span class="anomaly-time muted">{time}</span>
            <span class="anomaly-topic">{topic}</span>
        </div>
        <div class="anomaly-description">{description}</div>
    </div>"#,
                    severity_class = severity_class,
                    severity = severity,
                    time = time,
                    topic = topic,
                    description = html_escape(description),
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="anomalies-page">
    <div class="page-header">
        <p class="muted">Real-time anomaly detection across all streams.</p>
    </div>
    <div class="anomalies-list">
        {anomalies_html}
    </div>
</div>
"#,
            anomalies_html = anomalies_html,
        );

        self.layout(title, &content, "ai")
    }

    /// Render the edge sync status page.
    pub fn sync_status_page(&self, title: &str, nodes: &[(&str, &str, i32, &str)]) -> String {
        let nodes_html: String = nodes
            .iter()
            .map(|(node, status, pending, time)| {
                let status_class = match *status {
                    "synced" => "success",
                    "syncing" => "warning",
                    "pending" => "muted",
                    "error" => "error",
                    _ => "muted",
                };
                let pending_str = if *pending > 0 {
                    format!("{} pending", pending)
                } else {
                    "All synced".to_string()
                };
                format!(
                    r#"
    <tr>
        <td>{node}</td>
        <td><span class="status-badge status-{status_class}">{status}</span></td>
        <td>{pending_str}</td>
        <td class="muted">{time}</td>
    </tr>"#,
                    node = html_escape(node),
                    status_class = status_class,
                    status = status,
                    pending_str = pending_str,
                    time = time,
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="sync-status-page">
    <div class="page-header">
        <p class="muted">Edge node synchronization status.</p>
    </div>
    <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Node</th>
                    <th>Status</th>
                    <th>Pending</th>
                    <th>Last Activity</th>
                </tr>
            </thead>
            <tbody>
                {nodes_html}
            </tbody>
        </table>
    </div>
</div>
"#,
            nodes_html = nodes_html,
        );

        self.layout(title, &content, "edge")
    }

    /// Render the edge conflicts page.
    pub fn conflicts_page(&self, title: &str, conflicts: &[(&str, &str, &str, &str)]) -> String {
        let conflicts_html: String = conflicts
            .iter()
            .map(|(conflict_type, parties, status, time)| {
                let status_class = match *status {
                    "resolved" | "auto-resolved" => "success",
                    "pending" | "manual" => "warning",
                    _ => "muted",
                };
                format!(
                    r#"
    <tr>
        <td>{conflict_type}</td>
        <td>{parties}</td>
        <td><span class="status-badge status-{status_class}">{status}</span></td>
        <td class="muted">{time}</td>
        <td>
            <button class="btn btn-sm btn-secondary" onclick="resolveConflict('{conflict_type}')">Resolve</button>
        </td>
    </tr>"#,
                    conflict_type = html_escape(conflict_type),
                    parties = html_escape(parties),
                    status_class = status_class,
                    status = status,
                    time = time,
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="conflicts-page">
    <div class="page-header">
        <p class="muted">Edge data conflicts requiring resolution.</p>
    </div>
    <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Parties</th>
                    <th>Status</th>
                    <th>Time</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {conflicts_html}
            </tbody>
        </table>
    </div>
</div>
<script>
function resolveConflict(conflictType) {{
    // Create and show conflict resolution modal
    let modal = document.getElementById('conflict-modal');
    if (!modal) {{
        modal = document.createElement('div');
        modal.id = 'conflict-modal';
        modal.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:1000';
        modal.innerHTML = `
            <div style="background:var(--bg-primary,#1e1e2e);border-radius:8px;padding:24px;max-width:500px;width:90%;color:var(--text-primary,#cdd6f4)">
                <h3 style="margin:0 0 16px">Resolve Conflict: <span id="conflict-type-label"></span></h3>
                <p style="margin:0 0 16px;opacity:0.8">Choose how to resolve this conflict:</p>
                <div style="display:flex;flex-direction:column;gap:8px">
                    <button onclick="submitResolution('accept-local')" style="padding:10px;border:1px solid #45475a;border-radius:4px;background:#313244;color:#cdd6f4;cursor:pointer">
                        Accept Local — Keep the local version
                    </button>
                    <button onclick="submitResolution('accept-remote')" style="padding:10px;border:1px solid #45475a;border-radius:4px;background:#313244;color:#cdd6f4;cursor:pointer">
                        Accept Remote — Use the remote version
                    </button>
                    <button onclick="submitResolution('manual-merge')" style="padding:10px;border:1px solid #45475a;border-radius:4px;background:#313244;color:#cdd6f4;cursor:pointer">
                        Manual Merge — Review and merge manually
                    </button>
                </div>
                <div style="margin-top:16px;text-align:right">
                    <button onclick="closeConflictModal()" style="padding:8px 16px;border:1px solid #45475a;border-radius:4px;background:transparent;color:#cdd6f4;cursor:pointer">Cancel</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
    }}
    document.getElementById('conflict-type-label').textContent = conflictType;
    modal.dataset.conflictType = conflictType;
    modal.style.display = 'flex';
}}

function closeConflictModal() {{
    const modal = document.getElementById('conflict-modal');
    if (modal) modal.style.display = 'none';
}}

function submitResolution(strategy) {{
    const modal = document.getElementById('conflict-modal');
    const conflictType = modal ? modal.dataset.conflictType : '';
    fetch('/api/v1/edge/conflicts/resolve', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ conflict_type: conflictType, strategy: strategy }})
    }}).then(resp => {{
        if (resp.ok) {{
            closeConflictModal();
            location.reload();
        }} else {{
            alert('Failed to resolve conflict: ' + resp.statusText);
        }}
    }}).catch(err => alert('Error: ' + err));
}}
</script>
"#,
            conflicts_html = conflicts_html,
        );

        self.layout(title, &content, "edge")
    }

    /// Render the SQL query page for Lakehouse.
    pub fn query_page(&self, title: &str, description: &str, default_query: &str) -> String {
        let content = format!(
            r#"
<div class="query-page">
    <div class="page-header">
        <p class="muted">{description}</p>
    </div>
    <div class="query-editor">
        <div class="form-group">
            <label for="sql-query">SQL Query</label>
            <textarea id="sql-query" class="form-textarea" rows="6">{default_query}</textarea>
        </div>
        <div class="query-actions">
            <button class="btn btn-primary" onclick="executeQuery()">Execute Query</button>
            <button class="btn btn-secondary" onclick="clearQuery()">Clear</button>
        </div>
    </div>
    <div class="query-results" id="query-results">
        <p class="muted">Enter a SQL query and click Execute to see results.</p>
    </div>
</div>
<script>
async function executeQuery() {{
    const query = document.getElementById('sql-query').value;
    const resultsDiv = document.getElementById('query-results');
    resultsDiv.innerHTML = '<div class="loading-spinner">Executing query...</div>';

    try {{
        const response = await fetch('/api/lakehouse/query', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ query }})
        }});
        const data = await response.json();

        if (data.error) {{
            resultsDiv.innerHTML = '<p class="error">' + data.error + '</p>';
            return;
        }}

        if (data.columns && data.rows) {{
            let tableHtml = '<table class="data-table"><thead><tr>';
            data.columns.forEach(col => {{
                tableHtml += '<th>' + col + '</th>';
            }});
            tableHtml += '</tr></thead><tbody>';
            data.rows.forEach(row => {{
                tableHtml += '<tr>';
                row.forEach(cell => {{
                    tableHtml += '<td>' + (cell !== null ? cell : 'NULL') + '</td>';
                }});
                tableHtml += '</tr>';
            }});
            tableHtml += '</tbody></table>';
            resultsDiv.innerHTML = '<div class="result-info">' + data.rows.length + ' rows returned</div><div class="table-container">' + tableHtml + '</div>';
        }} else {{
            resultsDiv.innerHTML = '<p class="muted">Query executed successfully with no results.</p>';
        }}
    }} catch (e) {{
        resultsDiv.innerHTML = '<p class="error">Query failed: ' + e.message + '</p>';
    }}
}}

function clearQuery() {{
    document.getElementById('sql-query').value = '';
    document.getElementById('query-results').innerHTML = '<p class="muted">Enter a SQL query and click Execute to see results.</p>';
}}
</script>
"#,
            description = description,
            default_query = html_escape(default_query),
        );

        self.layout(title, &content, "lakehouse")
    }

    /// Render the pipeline details page.
    pub fn pipeline_details_page(
        &self,
        name: &str,
        stages: &[(&str, &str, &str)],
        metrics: &[(&str, &str)],
    ) -> String {
        let stages_html: String = stages
            .iter()
            .enumerate()
            .map(|(i, (stage_name, component, status))| {
                let status_class = match *status {
                    "running" => "success",
                    "paused" => "warning",
                    "error" => "error",
                    _ => "muted",
                };
                let connector = if i < stages.len() - 1 {
                    r#"<div class="pipeline-connector">→</div>"#
                } else {
                    ""
                };
                format!(
                    r#"
    <div class="pipeline-stage">
        <div class="stage-header">
            <span class="stage-name">{stage_name}</span>
            <span class="status-badge status-{status_class}">{status}</span>
        </div>
        <div class="stage-component muted">{component}</div>
    </div>
    {connector}"#,
                    stage_name = stage_name,
                    status_class = status_class,
                    status = status,
                    component = component,
                    connector = connector,
                )
            })
            .collect();

        let metrics_html: String = metrics
            .iter()
            .map(|(label, value)| {
                format!(
                    r#"
    <div class="metric-item">
        <span class="metric-label">{label}</span>
        <span class="metric-value">{value}</span>
    </div>"#,
                    label = label,
                    value = value,
                )
            })
            .collect();

        let content = format!(
            r#"
<div class="pipeline-details-page">
    <div class="page-header">
        <h3>Pipeline: {name}</h3>
    </div>
    <div class="pipeline-flow">
        {stages_html}
    </div>
    <div class="pipeline-metrics">
        <h4>Metrics</h4>
        <div class="metrics-grid">
            {metrics_html}
        </div>
    </div>
    <div class="pipeline-actions">
        <button class="btn btn-warning" onclick="pausePipeline('{name}')">Pause</button>
        <button class="btn btn-error" onclick="stopPipeline('{name}')">Stop</button>
        <button class="btn btn-secondary" onclick="history.back()">Back</button>
    </div>
</div>
<script>
function pausePipeline(name) {{
    if (confirm('Pause pipeline ' + name + '?')) {{
        fetch('/api/pipelines/' + name + '/pause', {{ method: 'POST' }});
    }}
}}
function stopPipeline(name) {{
    if (confirm('Stop pipeline ' + name + '?')) {{
        fetch('/api/pipelines/' + name + '/stop', {{ method: 'POST' }});
    }}
}}
</script>
"#,
            name = html_escape(name),
            stages_html = stages_html,
            metrics_html = metrics_html,
        );

        self.layout(&format!("Pipeline: {}", name), &content, "pipelines")
    }
}

impl Default for Templates {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions for formatting

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_bytes_per_sec(bytes: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    if bytes >= GB {
        format!("{:.2} GB/s", bytes / GB)
    } else if bytes >= MB {
        format!("{:.2} MB/s", bytes / MB)
    } else if bytes >= KB {
        format!("{:.2} KB/s", bytes / KB)
    } else {
        format!("{:.0} B/s", bytes)
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn chrono_format_timestamp(ts: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let datetime = UNIX_EPOCH + Duration::from_millis(ts as u64);
    let now = std::time::SystemTime::now();
    let duration = now.duration_since(datetime).unwrap_or_default();

    if duration.as_secs() < 60 {
        format!("{}s ago", duration.as_secs())
    } else if duration.as_secs() < 3600 {
        format!("{}m ago", duration.as_secs() / 60)
    } else if duration.as_secs() < 86400 {
        format!("{}h ago", duration.as_secs() / 3600)
    } else {
        format!("{}d ago", duration.as_secs() / 86400)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Get an SVG icon for the given icon name.
fn get_icon(name: &str) -> &'static str {
    match name {
        "database" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>"#
        }
        "activity" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>"#
        }
        "clock" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>"#
        }
        "table" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="9" x2="9" y2="21"/><line x1="15" y1="9" x2="15" y2="21"/></svg>"#
        }
        "cpu" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>"#
        }
        "box" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>"#
        }
        "alert-triangle" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>"#
        }
        "alert-circle" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>"#
        }
        "search" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>"#
        }
        "server" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>"#
        }
        "refresh-cw" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>"#
        }
        "git-merge" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M6 21V9a9 9 0 0 0 9 9"/></svg>"#
        }
        "git-branch" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>"#
        }
        "wifi" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12.55a11 11 0 0 1 14.08 0"/><path d="M1.42 9a16 16 0 0 1 21.16 0"/><path d="M8.53 16.11a6 6 0 0 1 6.95 0"/><line x1="12" y1="20" x2="12.01" y2="20"/></svg>"#
        }
        "eye" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>"#
        }
        "upload-cloud" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0 0 18 9h-1.26A8 8 0 1 0 3 16.3"/><polyline points="16 16 12 12 8 16"/></svg>"#
        }
        "hard-drive" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="12" x2="2" y2="12"/><path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/><line x1="6" y1="16" x2="6.01" y2="16"/><line x1="10" y1="16" x2="10.01" y2="16"/></svg>"#
        }
        "terminal" => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>"#
        }
        _ => {
            r#"<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/></svg>"#
        }
    }
}

/// Format configuration value with human-readable display.
fn format_config_value(value: &str, unit: &str) -> String {
    match unit {
        "ms" => {
            if let Ok(ms) = value.parse::<i64>() {
                if ms < 0 {
                    format!("{} (forever)", value)
                } else if ms >= 86_400_000 {
                    let days = ms / 86_400_000;
                    format!(
                        "{} ({} day{})",
                        value,
                        days,
                        if days == 1 { "" } else { "s" }
                    )
                } else if ms >= 3_600_000 {
                    let hours = ms / 3_600_000;
                    format!(
                        "{} ({} hour{})",
                        value,
                        hours,
                        if hours == 1 { "" } else { "s" }
                    )
                } else if ms >= 60_000 {
                    let mins = ms / 60_000;
                    format!(
                        "{} ({} min{})",
                        value,
                        mins,
                        if mins == 1 { "" } else { "s" }
                    )
                } else if ms >= 1_000 {
                    let secs = ms / 1_000;
                    format!(
                        "{} ({} sec{})",
                        value,
                        secs,
                        if secs == 1 { "" } else { "s" }
                    )
                } else {
                    format!("{} ms", value)
                }
            } else {
                value.to_string()
            }
        }
        "bytes" => {
            if let Ok(bytes) = value.parse::<i64>() {
                if bytes < 0 {
                    format!("{} (unlimited)", value)
                } else if bytes >= 1_073_741_824 {
                    format!("{} ({:.1} GB)", value, bytes as f64 / 1_073_741_824.0)
                } else if bytes >= 1_048_576 {
                    format!("{} ({:.1} MB)", value, bytes as f64 / 1_048_576.0)
                } else if bytes >= 1024 {
                    format!("{} ({:.1} KB)", value, bytes as f64 / 1024.0)
                } else {
                    format!("{} bytes", value)
                }
            } else {
                value.to_string()
            }
        }
        _ => value.to_string(),
    }
}

/// Embedded CSS stylesheet.
const CSS: &str = r#"
:root {
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-tertiary: #334155;
    --text-primary: #f8fafc;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent-primary: #3b82f6;
    --accent-hover: #2563eb;
    --success: #22c55e;
    --warning: #eab308;
    --error: #ef4444;
    --border: #475569;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.6;
}

.container {
    display: flex;
    min-height: 100vh;
}

.sidebar {
    width: 240px;
    background: var(--bg-secondary);
    padding: 24px 16px;
    border-right: 1px solid var(--border);
    position: fixed;
    height: 100vh;
    overflow-y: auto;
}

.logo h1 {
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text-primary);
}

.logo .subtitle {
    font-size: 0.75rem;
    color: var(--text-muted);
    display: block;
    margin-top: 4px;
}

.nav-links {
    margin-top: 32px;
    display: flex;
    flex-direction: column;
    gap: 4px;
}

.nav-link {
    display: block;
    padding: 12px 16px;
    color: var(--text-secondary);
    text-decoration: none;
    border-radius: 8px;
    transition: all 0.2s;
}

.nav-link:hover {
    background: var(--bg-tertiary);
    color: var(--text-primary);
}

.nav-link.active {
    background: var(--accent-primary);
    color: white;
}

.content {
    flex: 1;
    margin-left: 240px;
    padding: 24px 32px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
}

.header h2 {
    font-size: 1.5rem;
    font-weight: 600;
}

.main-content {
    padding-bottom: 48px;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}

.stat-card {
    background: var(--bg-secondary);
    padding: 20px;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.stat-label {
    font-size: 0.875rem;
    color: var(--text-muted);
    margin-bottom: 8px;
}

.stat-value {
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--text-primary);
}

.stat-detail {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-top: 4px;
}

.dashboard-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
    gap: 24px;
}

.card {
    background: var(--bg-secondary);
    border-radius: 12px;
    border: 1px solid var(--border);
    overflow: hidden;
}

.card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px;
    border-bottom: 1px solid var(--border);
}

.card-header h3 {
    font-size: 1rem;
    font-weight: 600;
}

.card-body {
    padding: 20px;
}

.health-indicator {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 16px;
    border-radius: 8px;
    margin-bottom: 16px;
}

.health-indicator.healthy {
    background: rgba(34, 197, 94, 0.1);
}

.health-indicator.unhealthy {
    background: rgba(239, 68, 68, 0.1);
}

.health-dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
}

.healthy .health-dot {
    background: var(--success);
    box-shadow: 0 0 8px var(--success);
}

.unhealthy .health-dot {
    background: var(--error);
    box-shadow: 0 0 8px var(--error);
}

.health-text {
    font-weight: 600;
}

.healthy .health-text {
    color: var(--success);
}

.unhealthy .health-text {
    color: var(--error);
}

.health-details {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.health-item {
    display: flex;
    justify-content: space-between;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
}

.health-item:last-child {
    border-bottom: none;
}

.data-table {
    width: 100%;
    border-collapse: collapse;
}

.data-table th,
.data-table td {
    padding: 12px 16px;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

.data-table th {
    background: var(--bg-tertiary);
    font-weight: 600;
    font-size: 0.875rem;
    color: var(--text-secondary);
}

.data-table td {
    font-size: 0.875rem;
}

.data-table a {
    color: var(--accent-primary);
    text-decoration: none;
}

.data-table a:hover {
    text-decoration: underline;
}

.empty-state {
    text-align: center;
    color: var(--text-muted);
    padding: 32px !important;
}

.status-badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
}

.status-ok {
    background: rgba(34, 197, 94, 0.1);
    color: var(--success);
}

.status-warning {
    background: rgba(234, 179, 8, 0.1);
    color: var(--warning);
}

.status-error {
    background: rgba(239, 68, 68, 0.1);
    color: var(--error);
}

.status-pending {
    background: rgba(59, 130, 246, 0.1);
    color: var(--accent-primary);
}

.btn {
    display: inline-block;
    padding: 10px 20px;
    border-radius: 8px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    border: 1px solid var(--border);
    background: var(--bg-tertiary);
    color: var(--text-primary);
    text-decoration: none;
    transition: all 0.2s;
}

.btn:hover {
    background: var(--bg-secondary);
}

.btn-primary {
    background: var(--accent-primary);
    border-color: var(--accent-primary);
}

.btn-primary:hover {
    background: var(--accent-hover);
}

.btn-danger {
    background: var(--error);
    border-color: var(--error);
}

.btn-danger:hover {
    background: #dc2626;
}

.actions-bar {
    display: flex;
    gap: 12px;
    margin-bottom: 16px;
}

.breadcrumb {
    font-size: 0.875rem;
    color: var(--text-secondary);
    margin-bottom: 24px;
}

.breadcrumb a {
    color: var(--accent-primary);
    text-decoration: none;
}

.breadcrumb a:hover {
    text-decoration: underline;
}

.modal {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
}

.modal.hidden {
    display: none;
}

.modal-content {
    background: var(--bg-secondary);
    border-radius: 12px;
    border: 1px solid var(--border);
    width: 100%;
    max-width: 480px;
    max-height: 90vh;
    overflow-y: auto;
}

.modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px;
    border-bottom: 1px solid var(--border);
}

.modal-header h3 {
    font-size: 1.25rem;
    font-weight: 600;
}

.close-btn {
    background: none;
    border: none;
    font-size: 1.5rem;
    color: var(--text-muted);
    cursor: pointer;
}

.close-btn:hover {
    color: var(--text-primary);
}

.form-group {
    padding: 16px 20px;
}

.form-group label {
    display: block;
    font-size: 0.875rem;
    font-weight: 500;
    margin-bottom: 8px;
    color: var(--text-secondary);
}

.form-group input {
    width: 100%;
    padding: 10px 14px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-size: 0.875rem;
}

.form-group input:focus {
    outline: none;
    border-color: var(--accent-primary);
}

.form-row {
    display: flex;
    gap: 16px;
}

.form-row .form-group {
    flex: 1;
}

.form-actions {
    display: flex;
    justify-content: flex-end;
    gap: 12px;
    padding: 16px 20px;
    border-top: 1px solid var(--border);
}

.activity-feed {
    max-height: 300px;
    overflow-y: auto;
}

.muted {
    color: var(--text-muted);
}

.error {
    color: var(--error);
}

.warning {
    color: var(--warning);
}

.error-container {
    text-align: center;
    padding: 64px 24px;
}

.error-container h2 {
    margin-bottom: 16px;
}

.error-container p {
    color: var(--text-secondary);
    margin-bottom: 24px;
}

.metrics-output {
    background: var(--bg-primary);
    padding: 16px;
    border-radius: 8px;
    font-family: monospace;
    font-size: 0.75rem;
    white-space: pre-wrap;
    max-height: 600px;
    overflow-y: auto;
    color: var(--text-secondary);
}

/* Log Viewer Page */
.log-controls {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    align-items: center;
}

.log-controls .control-group {
    display: flex;
    align-items: center;
    gap: 6px;
}

.log-controls select,
.log-controls input[type="text"] {
    padding: 6px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-size: 0.875rem;
}

.log-controls input[type="text"] {
    width: 200px;
}

.log-viewer-container {
    padding: 0;
}

.log-output {
    background: var(--bg-primary);
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    font-size: 0.8rem;
    line-height: 1.5;
    height: 500px;
    overflow-y: auto;
    padding: 12px;
    border-radius: 4px;
}

.log-loading {
    color: var(--text-muted);
    padding: 20px;
    text-align: center;
}

.log-entry {
    display: flex;
    gap: 12px;
    padding: 4px 8px;
    border-radius: 2px;
    margin-bottom: 2px;
}

.log-entry:hover {
    background: var(--bg-secondary);
}

.log-entry.hidden {
    display: none;
}

.log-timestamp {
    color: var(--text-muted);
    white-space: nowrap;
    min-width: 80px;
}

.log-level {
    font-weight: 600;
    min-width: 50px;
    text-align: center;
    padding: 0 4px;
    border-radius: 2px;
}

.log-level-error {
    color: var(--error);
    background: rgba(239, 68, 68, 0.1);
}

.log-level-warn {
    color: var(--warning);
    background: rgba(245, 158, 11, 0.1);
}

.log-level-info {
    color: var(--accent-primary);
    background: rgba(59, 130, 246, 0.1);
}

.log-level-debug {
    color: var(--text-muted);
    background: rgba(107, 114, 128, 0.1);
}

.log-level-trace {
    color: var(--text-muted);
    opacity: 0.7;
}

.log-target {
    color: var(--accent-secondary);
    white-space: nowrap;
    max-width: 150px;
    overflow: hidden;
    text-overflow: ellipsis;
}

.log-message {
    color: var(--text-primary);
    flex: 1;
    word-break: break-word;
}

.log-stats {
    display: flex;
    gap: 24px;
    flex-wrap: wrap;
}

.log-stat {
    display: flex;
    align-items: center;
    gap: 8px;
}

.log-stat-label {
    color: var(--text-muted);
}

.log-stat-value {
    font-weight: 600;
    font-size: 1.1rem;
}

/* Connection inspector styles */
.connection-stats .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}

.connection-stats .stat-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 20px;
    text-align: center;
}

.connection-stats .stat-value {
    font-size: 2rem;
    font-weight: 700;
    color: var(--text-primary);
    margin-bottom: 8px;
}

.connection-stats .stat-label {
    color: var(--text-muted);
    font-size: 0.875rem;
}

.connection-details .details-table {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 16px;
}

.connection-details .detail-row {
    display: flex;
    justify-content: space-between;
    padding: 12px 0;
    border-bottom: 1px solid var(--border-color);
}

.connection-details .detail-row:last-child {
    border-bottom: none;
}

.connection-details .detail-label {
    color: var(--text-muted);
    font-weight: 500;
}

.connection-details .detail-value {
    color: var(--text-primary);
    font-weight: 600;
}

/* Alerts Page */
.alert-stats .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}

.alert-stats .stat-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    text-align: center;
}

.alert-stats .stat-value {
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--text-primary);
    margin-bottom: 4px;
}

.alert-stats .stat-label {
    color: var(--text-muted);
    font-size: 0.8rem;
}

.alert-cards {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 16px;
}

.alert-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
}

.alert-card.disabled {
    opacity: 0.6;
}

.alert-card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px;
    border-bottom: 1px solid var(--border);
}

.alert-card-header h3 {
    font-size: 1rem;
    font-weight: 600;
    margin: 0;
    display: flex;
    align-items: center;
    gap: 8px;
}

.alert-card-body {
    padding: 16px;
}

.alert-condition {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 12px;
}

.alert-condition .condition-type {
    font-size: 0.875rem;
    font-weight: 500;
    color: var(--text-primary);
}

.alert-condition .condition-details {
    font-size: 0.8rem;
    color: var(--text-muted);
}

.alert-threshold {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.875rem;
}

.alert-threshold .threshold-label {
    color: var(--text-muted);
}

.alert-threshold .threshold-value {
    font-weight: 600;
    color: var(--accent-primary);
}

.alert-card-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    background: var(--bg-tertiary);
    border-top: 1px solid var(--border);
}

.alert-card-footer .alert-actions {
    display: flex;
    gap: 8px;
}

.alert-card-footer .btn {
    padding: 6px 12px;
    font-size: 0.8rem;
}

.severity-badge {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
}

.severity-badge.warning {
    background: rgba(245, 158, 11, 0.2);
    color: var(--warning);
}

.severity-badge.critical {
    background: rgba(239, 68, 68, 0.2);
    color: var(--error);
}

.enabled-badge {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
}

.enabled-badge.enabled {
    background: rgba(34, 197, 94, 0.2);
    color: var(--success);
}

.enabled-badge.disabled {
    background: rgba(107, 114, 128, 0.2);
    color: var(--text-muted);
}

.alert-history-table {
    margin-top: 24px;
}

.alert-history-table .event-severity {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 8px;
}

.alert-history-table .event-severity.warning {
    background: var(--warning);
}

.alert-history-table .event-severity.critical {
    background: var(--error);
}

/* Alert form modal */
.alert-form .form-group select {
    width: 100%;
    padding: 10px 14px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-size: 0.875rem;
}

.alert-form .form-group select:focus {
    outline: none;
    border-color: var(--accent-primary);
}

.alert-form .conditional-fields {
    padding: 0 20px;
}

.alert-form .conditional-fields .form-group {
    padding: 8px 0;
}

.channels-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 0 20px 16px;
}

.channel-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    background: var(--bg-tertiary);
    border-radius: 6px;
}

.channel-item input[type="checkbox"] {
    width: 16px;
    height: 16px;
}

.channel-item label {
    flex: 1;
    font-size: 0.875rem;
}

.channel-item input[type="text"] {
    flex: 2;
    padding: 6px 10px;
    border-radius: 4px;
    border: 1px solid var(--border);
    background: var(--bg-secondary);
    color: var(--text-primary);
    font-size: 0.8rem;
}

/* Topic Comparison Page */
.compare-selector {
    display: flex;
    align-items: flex-end;
    gap: 20px;
    flex-wrap: wrap;
}

.compare-topic-select {
    flex: 1;
    min-width: 200px;
}

.compare-topic-select label {
    display: block;
    font-size: 0.875rem;
    font-weight: 500;
    margin-bottom: 8px;
    color: var(--text-secondary);
}

.compare-topic-select select {
    width: 100%;
    padding: 10px 14px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-size: 0.875rem;
}

.compare-topic-select select:focus {
    outline: none;
    border-color: var(--accent-primary);
}

.compare-vs {
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--text-muted);
    padding-bottom: 10px;
}

.comparison-section {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
    margin-top: 24px;
}

.comparison-section h3 {
    font-size: 1.1rem;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--text-primary);
}

.comparison-metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
}

.comparison-metric {
    background: var(--bg-tertiary);
    border-radius: 8px;
    padding: 16px;
    text-align: center;
}

.comparison-metric .metric-label {
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-bottom: 8px;
}

.comparison-metric .metric-values {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 12px;
    font-size: 1.25rem;
    font-weight: 600;
}

.comparison-metric .metric-values .vs {
    font-size: 0.8rem;
    color: var(--text-muted);
    font-weight: 400;
}

.comparison-metric .metric-values .leader {
    color: var(--success);
}

.comparison-table {
    margin-top: 0;
}

.comparison-table .diff-highlight {
    background: rgba(245, 158, 11, 0.1);
}

.comparison-details-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
}

.comparison-topic-card {
    background: var(--bg-tertiary);
    border-radius: 8px;
    padding: 20px;
}

.comparison-topic-card h4 {
    font-size: 1rem;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--accent-primary);
}

.comparison-topic-card .topic-stat {
    display: flex;
    justify-content: space-between;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
    font-size: 0.875rem;
}

.comparison-topic-card .topic-stat:last-of-type {
    border-bottom: none;
    margin-bottom: 12px;
}

.comparison-topic-card .topic-stat .label {
    color: var(--text-muted);
}

.comparison-topic-card .btn-sm {
    padding: 6px 12px;
    font-size: 0.8rem;
}

/* Benchmark styles */
.benchmark-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 20px;
}

@media (max-width: 1200px) {
    .benchmark-grid {
        grid-template-columns: 1fr;
    }
}

.benchmark-form .form-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}

.active-benchmark-info {
    padding: 16px;
}

.benchmark-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
}

.benchmark-id {
    font-size: 0.875rem;
}

.benchmark-details {
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
    font-size: 0.875rem;
    margin-bottom: 16px;
}

.progress-container {
    background: var(--bg-tertiary);
    border-radius: 8px;
    height: 16px;
    overflow: hidden;
    margin-bottom: 8px;
}

.progress-bar {
    background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
    height: 100%;
    border-radius: 8px;
    transition: width 0.3s ease;
}

.progress-text {
    font-size: 0.8rem;
    color: var(--text-muted);
    text-align: center;
    margin-bottom: 16px;
}

.benchmark-results-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px;
    margin-top: 16px;
}

.result-item {
    background: var(--bg-tertiary);
    border-radius: 6px;
    padding: 12px;
    text-align: center;
}

.result-label {
    display: block;
    font-size: 0.75rem;
    color: var(--text-muted);
    margin-bottom: 4px;
}

.result-value {
    display: block;
    font-size: 1rem;
    font-weight: 600;
    color: var(--text-primary);
}

.benchmark-modal-info p {
    margin: 8px 0;
}

.page-desc {
    color: var(--text-secondary);
    font-size: 0.875rem;
    margin-top: 4px;
}

/* Search box styles */
.search-box {
    display: flex;
    align-items: center;
    margin-bottom: 16px;
}

.search-input {
    width: 100%;
    max-width: 400px;
    padding: 10px 14px 10px 40px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-size: 0.875rem;
}

.search-input:focus {
    outline: none;
    border-color: var(--accent-primary);
}

.search-input::placeholder {
    color: var(--text-muted);
}

.search-icon {
    position: absolute;
    left: 12px;
    color: var(--text-muted);
    pointer-events: none;
}

.search-wrapper {
    position: relative;
    display: inline-flex;
    align-items: center;
    flex: 1;
    max-width: 400px;
}

.search-shortcut {
    position: absolute;
    right: 10px;
    padding: 2px 6px;
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 4px;
    font-size: 0.7rem;
    color: var(--text-muted);
    font-family: monospace;
}

/* Keyboard shortcuts help modal */
.shortcuts-modal {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 2000;
}

.shortcuts-modal.hidden {
    display: none;
}

.shortcuts-content {
    background: var(--bg-secondary);
    border-radius: 12px;
    border: 1px solid var(--border);
    padding: 24px;
    max-width: 400px;
}

.shortcuts-content h3 {
    margin-bottom: 16px;
    font-size: 1.25rem;
}

.shortcut-item {
    display: flex;
    justify-content: space-between;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
}

.shortcut-item:last-child {
    border-bottom: none;
}

.shortcut-key {
    padding: 4px 8px;
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 4px;
    font-size: 0.75rem;
    font-family: monospace;
    color: var(--text-primary);
}

.shortcut-desc {
    color: var(--text-secondary);
}

/* Empty state improvements */
.empty-state-container {
    text-align: center;
    padding: 48px 24px;
}

.empty-state-icon {
    font-size: 3rem;
    margin-bottom: 16px;
    opacity: 0.5;
}

.empty-state-title {
    font-size: 1.125rem;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 8px;
}

.empty-state-desc {
    color: var(--text-muted);
    margin-bottom: 16px;
}

/* Row highlight for search matches */
.data-table tr.search-match td {
    background: rgba(59, 130, 246, 0.1);
}

.data-table tr.search-hidden {
    display: none;
}

/* Sparkline charts */
.sparkline-container {
    height: 40px;
    margin-top: 8px;
}

.sparkline {
    width: 100%;
    height: 100%;
}

.sparkline-line {
    fill: none;
    stroke: var(--accent-primary);
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
}

.sparkline-area {
    fill: url(#sparkline-gradient);
    opacity: 0.3;
}

.sparkline-dot {
    fill: var(--accent-primary);
}

.stat-card-with-chart {
    padding-bottom: 12px;
}

.stat-card-with-chart .stat-value {
    font-size: 1.5rem;
}

/* Toast notifications */
.toast-container {
    position: fixed;
    top: 20px;
    right: 20px;
    z-index: 3000;
    display: flex;
    flex-direction: column;
    gap: 10px;
}

.toast {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 16px;
    min-width: 280px;
    max-width: 400px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    display: flex;
    align-items: center;
    gap: 12px;
    animation: slideIn 0.3s ease;
}

.toast.toast-success {
    border-left: 4px solid var(--success);
}

.toast.toast-error {
    border-left: 4px solid var(--error);
}

.toast.toast-warning {
    border-left: 4px solid var(--warning);
}

.toast.toast-info {
    border-left: 4px solid var(--accent-primary);
}

.toast-icon {
    font-size: 1.25rem;
}

.toast-message {
    flex: 1;
    font-size: 0.875rem;
}

.toast-close {
    background: none;
    border: none;
    color: var(--text-muted);
    cursor: pointer;
    font-size: 1.25rem;
    padding: 0;
    line-height: 1;
}

.toast-close:hover {
    color: var(--text-primary);
}

@keyframes slideIn {
    from {
        transform: translateX(100%);
        opacity: 0;
    }
    to {
        transform: translateX(0);
        opacity: 1;
    }
}

@keyframes slideOut {
    from {
        transform: translateX(0);
        opacity: 1;
    }
    to {
        transform: translateX(100%);
        opacity: 0;
    }
}

/* Header actions (refresh toggle, connection status, theme) */
.header-controls {
    display: flex;
    align-items: center;
    gap: 16px;
}

.connection-status {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.75rem;
    color: var(--text-muted);
}

.connection-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--text-muted);
}

.connection-dot.connected {
    background: var(--success);
    box-shadow: 0 0 6px var(--success);
}

.connection-dot.disconnected {
    background: var(--error);
}

.connection-dot.connecting {
    background: var(--warning);
    animation: pulse 1s infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

.refresh-toggle {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.75rem;
    color: var(--text-muted);
}

.toggle-switch {
    position: relative;
    width: 36px;
    height: 20px;
    background: var(--bg-tertiary);
    border-radius: 10px;
    cursor: pointer;
    transition: background 0.2s;
}

.toggle-switch.active {
    background: var(--accent-primary);
}

.toggle-switch::after {
    content: '';
    position: absolute;
    top: 2px;
    left: 2px;
    width: 16px;
    height: 16px;
    background: white;
    border-radius: 50%;
    transition: transform 0.2s;
}

.toggle-switch.active::after {
    transform: translateX(16px);
}

.theme-selector {
    position: relative;
}

.theme-selector select {
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 28px 6px 10px;
    cursor: pointer;
    color: var(--text-primary);
    font-size: 0.875rem;
    transition: all 0.2s;
    appearance: none;
    -webkit-appearance: none;
    -moz-appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%23888' d='M6 8L2 4h8z'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 8px center;
}

.theme-selector select:hover {
    border-color: var(--accent-primary);
}

.theme-selector select:focus {
    outline: none;
    border-color: var(--accent-primary);
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
}

.theme-selector select option {
    background: var(--bg-secondary);
    color: var(--text-primary);
    padding: 8px;
}

/* Pagination */
.pagination {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    border-top: 1px solid var(--border);
    font-size: 0.875rem;
}

.pagination-info {
    color: var(--text-muted);
}

.pagination-controls {
    display: flex;
    align-items: center;
    gap: 8px;
}

.pagination-btn {
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 6px 12px;
    color: var(--text-secondary);
    cursor: pointer;
    font-size: 0.75rem;
    transition: all 0.2s;
}

.pagination-btn:hover:not(:disabled) {
    background: var(--bg-secondary);
    color: var(--text-primary);
}

.pagination-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.pagination-select {
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 6px 8px;
    color: var(--text-primary);
    font-size: 0.75rem;
}

/* Favorites star */
.favorite-btn {
    background: none;
    border: none;
    cursor: pointer;
    font-size: 1rem;
    color: var(--text-muted);
    padding: 2px;
    transition: color 0.2s;
}

.favorite-btn:hover {
    color: var(--warning);
}

.favorite-btn.active {
    color: var(--warning);
}

/* Light theme */
body.light-theme {
    --bg-primary: #f8fafc;
    --bg-secondary: #ffffff;
    --bg-tertiary: #e2e8f0;
    --text-primary: #1e293b;
    --text-secondary: #475569;
    --text-muted: #94a3b8;
    --border: #cbd5e1;
}

/* High contrast theme (WCAG AAA) */
body.high-contrast-theme {
    --bg-primary: #000000;
    --bg-secondary: #1a1a1a;
    --bg-tertiary: #333333;
    --text-primary: #ffffff;
    --text-secondary: #e0e0e0;
    --text-muted: #b0b0b0;
    --accent-primary: #00ccff;
    --accent-hover: #00aadd;
    --accent-secondary: #00ccff;
    --success: #00ff00;
    --warning: #ffff00;
    --error: #ff0000;
    --border: #666666;
}

/* Colorblind-friendly theme (Deuteranopia/Protanopia safe) */
body.colorblind-theme {
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-tertiary: #334155;
    --text-primary: #f8fafc;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent-primary: #0077bb;
    --accent-hover: #005588;
    --accent-secondary: #0077bb;
    --success: #009988;
    --warning: #ee7733;
    --error: #cc3311;
    --border: #475569;
}

/* Solarized light theme */
body.solarized-light-theme {
    --bg-primary: #fdf6e3;
    --bg-secondary: #eee8d5;
    --bg-tertiary: #ddd6c7;
    --text-primary: #073642;
    --text-secondary: #586e75;
    --text-muted: #93a1a1;
    --accent-primary: #268bd2;
    --accent-hover: #2176bd;
    --success: #859900;
    --warning: #b58900;
    --error: #dc322f;
    --border: #93a1a1;
}

/* Solarized dark theme */
body.solarized-dark-theme {
    --bg-primary: #002b36;
    --bg-secondary: #073642;
    --bg-tertiary: #094152;
    --text-primary: #fdf6e3;
    --text-secondary: #93a1a1;
    --text-muted: #657b83;
    --accent-primary: #268bd2;
    --accent-hover: #2aa198;
    --success: #859900;
    --warning: #b58900;
    --error: #dc322f;
    --border: #586e75;
}

@media (max-width: 768px) {
    .sidebar {
        position: relative;
        width: 100%;
        height: auto;
    }

    .content {
        margin-left: 0;
    }

    .dashboard-grid {
        grid-template-columns: 1fr;
    }

    .form-row {
        flex-direction: column;
    }
}

/* Heatmap Styles */
.heatmap-container {
    overflow: auto;
    max-height: 70vh;
}

.heatmap-table {
    border-collapse: collapse;
    width: 100%;
    min-width: 600px;
}

.heatmap-corner {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    min-width: 120px;
    position: sticky;
    left: 0;
    z-index: 2;
}

.heatmap-header {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    padding: 0.5rem;
    font-weight: 500;
    font-size: 0.85rem;
    text-align: center;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 120px;
    position: sticky;
    top: 0;
    z-index: 1;
}

.heatmap-row-header {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    padding: 0.5rem;
    font-weight: 500;
    font-size: 0.85rem;
    text-align: left;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 150px;
    position: sticky;
    left: 0;
    z-index: 1;
}

.heatmap-cell {
    border: 1px solid var(--border);
    padding: 0.5rem;
    text-align: center;
    font-weight: 600;
    font-size: 0.85rem;
    cursor: pointer;
    transition: transform 0.1s, box-shadow 0.1s;
    min-width: 80px;
}

.heatmap-cell:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    z-index: 1;
}

.heatmap-empty {
    background: var(--bg-secondary);
    color: var(--text-muted);
    cursor: default;
}

.heatmap-empty:hover {
    transform: none;
    box-shadow: none;
}

.severity-low {
    background: #22c55e;
    color: white;
}

.severity-medium {
    background: #eab308;
    color: #1a1a1a;
}

.severity-high {
    background: #f97316;
    color: white;
}

.severity-critical {
    background: #ef4444;
    color: white;
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.8; }
}

.heatmap-legend {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
}

.legend-item {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.85rem;
    color: var(--text-secondary);
}

.legend-color {
    width: 16px;
    height: 16px;
    border-radius: 3px;
    display: inline-block;
}

.legend-color.severity-low { background: #22c55e; }
.legend-color.severity-medium { background: #eab308; }
.legend-color.severity-high { background: #f97316; }
.legend-color.severity-critical { background: #ef4444; }

#heatmap-details {
    min-height: 100px;
}

.heatmap-detail-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
}

.heatmap-detail-item {
    padding: 1rem;
    background: var(--bg-secondary);
    border-radius: 8px;
}

.heatmap-detail-item .label {
    font-size: 0.85rem;
    color: var(--text-muted);
    margin-bottom: 0.25rem;
}

.heatmap-detail-item .value {
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--text-primary);
}

.heatmap-cell.selected {
    outline: 3px solid var(--accent-primary);
    outline-offset: -3px;
}

/* Message Browser Styles */
.browse-controls {
    padding: 1rem;
    margin-bottom: 1rem;
}

.browse-controls .form-row {
    display: flex;
    gap: 1rem;
    align-items: flex-end;
    flex-wrap: wrap;
}

.browse-controls .form-group {
    flex: 1;
    min-width: 120px;
}

.browse-controls .form-group label {
    display: block;
    margin-bottom: 0.5rem;
    font-size: 0.85rem;
    color: var(--text-secondary);
}

.browse-controls .form-group select,
.browse-controls .form-group input {
    width: 100%;
    padding: 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-tertiary);
    color: var(--text-primary);
}

.browse-controls .form-actions {
    flex: 0 0 auto;
}

.messages-container {
    margin-bottom: 1rem;
}

.message-card {
    background: var(--bg-secondary);
    border-radius: 8px;
    margin-bottom: 1rem;
    overflow: hidden;
}

.message-header {
    display: flex;
    justify-content: space-between;
    padding: 0.75rem 1rem;
    background: var(--bg-tertiary);
    border-bottom: 1px solid var(--border);
}

.message-offset {
    font-weight: 600;
    color: var(--accent-primary);
}

.message-timestamp {
    color: var(--text-muted);
    font-size: 0.85rem;
}

.message-meta {
    display: flex;
    gap: 2rem;
    padding: 0.75rem 1rem;
    font-size: 0.9rem;
    color: var(--text-secondary);
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
}

.message-value {
    padding: 1rem;
}

.message-value pre {
    margin: 0;
    padding: 1rem;
    background: var(--bg-primary);
    border-radius: 4px;
    overflow-x: auto;
}

.message-value code {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.85rem;
    color: var(--text-primary);
}

.pagination {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 1rem;
    margin-top: 1rem;
}

.page-info {
    color: var(--text-secondary);
}

.pagination .btn[disabled] {
    opacity: 0.5;
    pointer-events: none;
}

/* Producer Form Styles */
#produce-form .form-group {
    margin-bottom: 1rem;
}

#produce-form label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 500;
    color: var(--text-secondary);
}

#produce-form input,
#produce-form select,
#produce-form textarea {
    width: 100%;
    padding: 0.75rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-tertiary);
    color: var(--text-primary);
    font-family: inherit;
}

#produce-form textarea {
    font-family: 'Monaco', 'Consolas', monospace;
    resize: vertical;
}

#produce-form .form-actions {
    display: flex;
    gap: 1rem;
    margin-top: 1.5rem;
}

.header-row {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.5rem;
}

.header-row input {
    flex: 1;
}

.header-row .btn {
    flex-shrink: 0;
}

.btn-small {
    padding: 0.25rem 0.5rem;
    font-size: 0.85rem;
}

.validation-message {
    font-size: 0.85rem;
    margin-top: 0.25rem;
}

.validation-message.valid {
    color: var(--success);
}

.validation-message.invalid {
    color: var(--error);
}

#produce-results {
    margin-top: 1.5rem;
}

#results-list {
    padding: 1rem;
}

.result-item {
    display: flex;
    justify-content: space-between;
    padding: 0.75rem;
    background: var(--bg-tertiary);
    border-radius: 4px;
    margin-bottom: 0.5rem;
}

.result-item .success {
    color: var(--success);
}

.result-item .error {
    color: var(--error);
}

/* Topic Configuration Page */
.config-page {
    display: flex;
    flex-direction: column;
    gap: 1.5rem;
}

.config-table {
    width: 100%;
    border-collapse: collapse;
}

.config-table th,
.config-table td {
    padding: 1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

.config-table th {
    background: var(--bg-tertiary);
    font-weight: 600;
    color: var(--text-secondary);
    font-size: 0.875rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.config-key {
    min-width: 300px;
}

.config-key code {
    background: var(--bg-tertiary);
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-family: monospace;
    color: var(--accent-primary);
    font-size: 0.9rem;
}

.config-desc {
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-top: 0.5rem;
}

.config-value {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.config-value-text {
    font-family: monospace;
    color: var(--text-primary);
}

.config-badge {
    font-size: 0.7rem;
    padding: 0.2rem 0.5rem;
    border-radius: 4px;
    text-transform: uppercase;
    font-weight: 600;
}

.config-badge.default {
    background: var(--bg-tertiary);
    color: var(--text-muted);
}

.config-badge.custom {
    background: rgba(59, 130, 246, 0.2);
    color: var(--accent-primary);
}

.config-info {
    font-size: 0.875rem;
    color: var(--text-secondary);
}

.preset-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1rem;
    padding: 1rem;
}

.preset-card {
    background: var(--bg-tertiary);
    padding: 1.25rem;
    border-radius: 8px;
    border: 1px solid var(--border);
    transition: border-color 0.2s, transform 0.2s;
}

.preset-card:hover {
    border-color: var(--accent-primary);
    transform: translateY(-2px);
}

.preset-card h4 {
    color: var(--text-primary);
    margin-bottom: 0.5rem;
    font-size: 1rem;
}

.preset-card p {
    color: var(--text-secondary);
    font-size: 0.8rem;
    margin-bottom: 1rem;
}

.preset-values {
    list-style: none;
    font-size: 0.8rem;
}

.preset-values li {
    padding: 0.35rem 0;
    color: var(--text-muted);
    border-top: 1px solid var(--border);
}

.preset-values li:first-child {
    border-top: none;
}

.preset-values code {
    background: var(--bg-secondary);
    padding: 0.15rem 0.35rem;
    border-radius: 3px;
    font-size: 0.75rem;
    color: var(--accent-primary);
}

/* Offset Management Page */
.form-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
}

.preview-summary {
    padding: 1rem;
    background: var(--bg-tertiary);
    border-radius: 4px;
    margin-bottom: 1rem;
}

.preview-summary p {
    margin: 0.5rem 0;
    color: var(--text-secondary);
}

.preview-summary strong {
    color: var(--text-primary);
}

.partition-checkbox {
    width: 18px;
    height: 18px;
    cursor: pointer;
}

#select-all {
    width: 18px;
    height: 18px;
    cursor: pointer;
}
"#;

/// Embedded JavaScript.
const JS: &str = r#"
// Toast notification system
function showToast(message, type = 'info', duration = 4000) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const icons = {
        success: '✓',
        error: '✕',
        warning: '⚠',
        info: 'ℹ'
    };

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;

    container.appendChild(toast);

    // Auto-remove after duration
    setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease forwards';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// Available themes
const THEMES = ['dark', 'light', 'high-contrast', 'colorblind', 'solarized-light', 'solarized-dark'];

// Set theme
function setTheme(theme) {
    const body = document.body;
    const select = document.getElementById('theme-select');

    // Remove all theme classes
    THEMES.forEach(t => {
        if (t !== 'dark') {
            body.classList.remove(t + '-theme');
        }
    });

    // Apply new theme (dark is default, no class needed)
    if (theme !== 'dark') {
        body.classList.add(theme + '-theme');
    }

    // Update select element
    if (select) {
        select.value = theme;
    }

    // Persist theme preference
    localStorage.setItem('streamline-theme', theme);
}

// Load saved theme
function loadTheme() {
    const savedTheme = localStorage.getItem('streamline-theme') || 'dark';
    const select = document.getElementById('theme-select');

    // Apply the saved theme
    setTheme(savedTheme);

    // Ensure select shows correct value
    if (select) {
        select.value = savedTheme;
    }
}

// Auto-refresh toggle
let autoRefreshEnabled = true;

function toggleAutoRefresh() {
    const toggle = document.getElementById('auto-refresh-toggle');
    autoRefreshEnabled = !autoRefreshEnabled;

    if (toggle) {
        toggle.classList.toggle('active', autoRefreshEnabled);
    }

    // Save preference
    localStorage.setItem('streamline-auto-refresh', autoRefreshEnabled);

    // Show feedback
    showToast(autoRefreshEnabled ? 'Auto-refresh enabled' : 'Auto-refresh disabled', 'info', 2000);

    // Reconnect or disconnect SSE based on preference
    if (autoRefreshEnabled && !eventSource) {
        connectSSE();
    } else if (!autoRefreshEnabled && eventSource) {
        eventSource.close();
        eventSource = null;
        updateConnectionStatus('disconnected', 'Paused');
    }
}

// Load auto-refresh preference
function loadAutoRefreshPreference() {
    const saved = localStorage.getItem('streamline-auto-refresh');
    if (saved !== null) {
        autoRefreshEnabled = saved === 'true';
        const toggle = document.getElementById('auto-refresh-toggle');
        if (toggle) {
            toggle.classList.toggle('active', autoRefreshEnabled);
        }
    }
}

// Connection status updates
function updateConnectionStatus(status, text) {
    const dot = document.getElementById('connection-dot');
    const textEl = document.getElementById('connection-text');

    if (dot) {
        dot.className = 'connection-dot ' + status;
    }
    if (textEl) {
        textEl.textContent = text || status;
    }
}

// Favorites system
function getFavorites() {
    try {
        return JSON.parse(localStorage.getItem('streamline-favorites') || '{}');
    } catch {
        return {};
    }
}

function saveFavorites(favorites) {
    localStorage.setItem('streamline-favorites', JSON.stringify(favorites));
}

function toggleFavorite(type, id) {
    const favorites = getFavorites();
    const key = `${type}:${id}`;

    if (favorites[key]) {
        delete favorites[key];
        showToast(`Removed "${id}" from favorites`, 'info', 2000);
    } else {
        favorites[key] = { type, id, addedAt: Date.now() };
        showToast(`Added "${id}" to favorites`, 'success', 2000);
    }

    saveFavorites(favorites);
    updateFavoriteButtons();
}

function isFavorite(type, id) {
    const favorites = getFavorites();
    return !!favorites[`${type}:${id}`];
}

function updateFavoriteButtons() {
    document.querySelectorAll('.favorite-btn').forEach(btn => {
        const type = btn.dataset.type;
        const id = btn.dataset.id;
        if (type && id) {
            btn.classList.toggle('active', isFavorite(type, id));
            btn.textContent = isFavorite(type, id) ? '★' : '☆';
        }
    });
}

// Pagination
const paginationState = {};

function initPagination(tableId, pageSize = 10) {
    paginationState[tableId] = {
        currentPage: 1,
        pageSize: pageSize,
        totalItems: 0
    };
    updatePagination(tableId);
}

function updatePagination(tableId) {
    const table = document.getElementById(tableId);
    if (!table) return;

    const state = paginationState[tableId];
    if (!state) return;

    const rows = Array.from(table.querySelectorAll('tr:not(.empty-state-row)'));
    state.totalItems = rows.length;

    const totalPages = Math.ceil(state.totalItems / state.pageSize) || 1;
    const start = (state.currentPage - 1) * state.pageSize;
    const end = start + state.pageSize;

    // Show/hide rows based on pagination
    rows.forEach((row, index) => {
        if (row.querySelector('.empty-state-container')) return;
        row.style.display = (index >= start && index < end) ? '' : 'none';
    });

    // Update pagination info
    const paginationEl = table.closest('.card')?.querySelector('.pagination');
    if (paginationEl) {
        const infoEl = paginationEl.querySelector('.pagination-info');
        const prevBtn = paginationEl.querySelector('.pagination-btn.prev');
        const nextBtn = paginationEl.querySelector('.pagination-btn.next');
        const pageInput = paginationEl.querySelector('.pagination-current');

        if (infoEl) {
            const showing = state.totalItems === 0 ? 0 : Math.min(start + 1, state.totalItems);
            const showingEnd = Math.min(end, state.totalItems);
            infoEl.textContent = `Showing ${showing}-${showingEnd} of ${state.totalItems}`;
        }
        if (prevBtn) prevBtn.disabled = state.currentPage <= 1;
        if (nextBtn) nextBtn.disabled = state.currentPage >= totalPages;
        if (pageInput) pageInput.textContent = `Page ${state.currentPage} of ${totalPages}`;
    }
}

function changePage(tableId, delta) {
    const state = paginationState[tableId];
    if (!state) return;

    const totalPages = Math.ceil(state.totalItems / state.pageSize) || 1;
    const newPage = state.currentPage + delta;

    if (newPage >= 1 && newPage <= totalPages) {
        state.currentPage = newPage;
        updatePagination(tableId);
    }
}

function changePageSize(tableId, newSize) {
    const state = paginationState[tableId];
    if (!state) return;

    state.pageSize = parseInt(newSize);
    state.currentPage = 1;
    updatePagination(tableId);
}

// Table filtering function
function filterTable(tableId, searchTerm) {
    const table = document.getElementById(tableId);
    if (!table) return;

    const rows = table.querySelectorAll('tr');
    const term = searchTerm.toLowerCase().trim();

    rows.forEach(row => {
        if (row.querySelector('.empty-state')) {
            // Skip empty state rows
            return;
        }

        const text = row.textContent.toLowerCase();
        if (term === '' || text.includes(term)) {
            row.classList.remove('search-hidden');
        } else {
            row.classList.add('search-hidden');
        }
    });

    // Update hidden shortcut hint when focused
    const searchInput = document.querySelector('.search-input');
    if (searchInput && document.activeElement === searchInput) {
        const shortcutHint = searchInput.parentElement.querySelector('.search-shortcut');
        if (shortcutHint) {
            shortcutHint.style.display = 'none';
        }
    }
}

// Keyboard shortcuts
let pendingKey = null;
let pendingTimeout = null;

function handleKeyboardShortcuts(e) {
    // Don't trigger shortcuts when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
        if (e.key === 'Escape') {
            e.target.blur();
            // Restore shortcut hint
            const shortcutHint = e.target.parentElement?.querySelector('.search-shortcut');
            if (shortcutHint) {
                shortcutHint.style.display = '';
            }
        }
        return;
    }

    // Handle two-key sequences (g + key)
    if (pendingKey === 'g') {
        clearTimeout(pendingTimeout);
        pendingKey = null;

        switch (e.key) {
            case 'd':
                window.location.href = '/';
                return;
            case 't':
                window.location.href = '/topics';
                return;
            case 'c':
                window.location.href = '/consumer-groups';
                return;
            case 'b':
                window.location.href = '/brokers';
                return;
            case 'm':
                window.location.href = '/metrics';
                return;
        }
    }

    // Handle single-key shortcuts
    switch (e.key) {
        case '/':
            e.preventDefault();
            const searchInput = document.querySelector('.search-input');
            if (searchInput) {
                searchInput.focus();
                // Hide shortcut hint when focused
                const shortcutHint = searchInput.parentElement.querySelector('.search-shortcut');
                if (shortcutHint) {
                    shortcutHint.style.display = 'none';
                }
            }
            break;

        case '?':
            e.preventDefault();
            toggleShortcutsModal();
            break;

        case 'r':
            e.preventDefault();
            window.location.reload();
            break;

        case 'Escape':
            closeAllModals();
            break;

        case 'g':
            // Start two-key sequence
            pendingKey = 'g';
            pendingTimeout = setTimeout(() => {
                pendingKey = null;
            }, 1000);
            break;
    }
}

function toggleShortcutsModal() {
    const modal = document.getElementById('shortcuts-modal');
    if (modal) {
        modal.classList.toggle('hidden');
    }
}

function closeAllModals() {
    const modals = document.querySelectorAll('.modal, .shortcuts-modal');
    modals.forEach(modal => modal.classList.add('hidden'));
}

// Sparkline data storage (last 20 data points)
const sparklineData = {
    messages: [],
    throughput: []
};
const MAX_SPARKLINE_POINTS = 20;

// Update sparkline SVG
function updateSparkline(svgId, data) {
    const svg = document.getElementById(svgId);
    if (!svg || data.length === 0) return;

    const width = 100;
    const height = 40;
    const padding = 2;

    // Normalize data to fit in the viewBox
    const max = Math.max(...data, 1); // Avoid division by zero
    const min = 0;
    const range = max - min || 1;

    // Calculate points
    const points = data.map((value, index) => {
        const x = (index / (MAX_SPARKLINE_POINTS - 1)) * width;
        const y = height - padding - ((value - min) / range) * (height - 2 * padding);
        return { x, y };
    });

    // Create line path
    const linePath = points.map((p, i) => (i === 0 ? `M${p.x},${p.y}` : `L${p.x},${p.y}`)).join(' ');

    // Create area path (closed polygon)
    const areaPath = linePath + ` L${points[points.length - 1].x},${height} L${points[0].x},${height} Z`;

    // Update SVG elements
    const lineEl = svg.querySelector('.sparkline-line');
    const areaEl = svg.querySelector('.sparkline-area');
    const dotEl = svg.querySelector('.sparkline-dot');

    if (lineEl) lineEl.setAttribute('d', linePath);
    if (areaEl) areaEl.setAttribute('d', areaPath);
    if (dotEl && points.length > 0) {
        const lastPoint = points[points.length - 1];
        dotEl.setAttribute('cx', lastPoint.x);
        dotEl.setAttribute('cy', lastPoint.y);
    }
}

// Add data point to sparkline
function addSparklinePoint(type, value) {
    const data = sparklineData[type];
    data.push(value);
    if (data.length > MAX_SPARKLINE_POINTS) {
        data.shift();
    }

    const svgId = type === 'messages' ? 'messages-sparkline' : 'throughput-sparkline';
    updateSparkline(svgId, data);
}

// Client-side cache for faster page navigation
const StreamlineCache = {
    config: {
        ttl: 30000,        // 30 second default TTL
        maxEntries: 100,   // Max cache entries
        storageKey: 'streamline-cache'
    },
    memoryCache: new Map(),
    timestamps: new Map(),

    // Get cached value if still valid
    get(key) {
        const timestamp = this.timestamps.get(key);
        if (!timestamp) return null;

        const age = Date.now() - timestamp;
        if (age > this.config.ttl) {
            this.memoryCache.delete(key);
            this.timestamps.delete(key);
            return null;
        }

        return this.memoryCache.get(key);
    },

    // Set cached value with optional custom TTL
    set(key, data, customTtl) {
        // Evict oldest entries if at max capacity
        if (this.memoryCache.size >= this.config.maxEntries) {
            const oldest = [...this.timestamps.entries()].sort((a, b) => a[1] - b[1])[0];
            if (oldest) {
                this.memoryCache.delete(oldest[0]);
                this.timestamps.delete(oldest[0]);
            }
        }

        this.memoryCache.set(key, data);
        this.timestamps.set(key, Date.now());

        // Persist to localStorage for page refreshes
        this.saveToStorage();
    },

    // Invalidate specific key or pattern
    invalidate(keyOrPattern) {
        if (typeof keyOrPattern === 'string') {
            this.memoryCache.delete(keyOrPattern);
            this.timestamps.delete(keyOrPattern);
        } else if (keyOrPattern instanceof RegExp) {
            for (const key of this.memoryCache.keys()) {
                if (keyOrPattern.test(key)) {
                    this.memoryCache.delete(key);
                    this.timestamps.delete(key);
                }
            }
        }
        this.saveToStorage();
    },

    // Invalidate all cached data
    invalidateAll() {
        this.memoryCache.clear();
        this.timestamps.clear();
        localStorage.removeItem(this.config.storageKey);
    },

    // Save cache to localStorage
    saveToStorage() {
        try {
            const data = {
                entries: [...this.memoryCache.entries()],
                timestamps: [...this.timestamps.entries()]
            };
            localStorage.setItem(this.config.storageKey, JSON.stringify(data));
        } catch (e) {
            // localStorage might be full or disabled
            console.warn('Failed to persist cache:', e);
        }
    },

    // Load cache from localStorage
    loadFromStorage() {
        try {
            const stored = localStorage.getItem(this.config.storageKey);
            if (stored) {
                const data = JSON.parse(stored);
                const now = Date.now();

                // Restore only non-expired entries
                data.entries.forEach(([key, value], idx) => {
                    const timestamp = data.timestamps[idx]?.[1] || 0;
                    if (now - timestamp < this.config.ttl) {
                        this.memoryCache.set(key, value);
                        this.timestamps.set(key, timestamp);
                    }
                });
            }
        } catch (e) {
            console.warn('Failed to load cache:', e);
        }
    },

    // Get cache stats for debugging
    getStats() {
        return {
            entries: this.memoryCache.size,
            maxEntries: this.config.maxEntries,
            ttl: this.config.ttl
        };
    }
};

// Initialize cache from storage
StreamlineCache.loadFromStorage();

// SSE connection for real-time updates
let eventSource = null;

function connectSSE() {
    if (eventSource) {
        eventSource.close();
    }

    eventSource = new EventSource('/api/events');

    eventSource.onopen = function() {
        console.log('SSE connected');
        updateConnectionStatus('connected', 'Connected');
        showToast('Connected to server', 'success', 2000);
    };

    eventSource.onerror = function(e) {
        console.error('SSE error:', e);
        updateConnectionStatus('disconnected', 'Disconnected');
        if (autoRefreshEnabled) {
            updateConnectionStatus('connecting', 'Reconnecting...');
            setTimeout(connectSSE, 5000);
        }
    };

    eventSource.addEventListener('cluster', function(e) {
        const data = JSON.parse(e.data);
        // Cache the cluster data for faster page loads
        StreamlineCache.set('api:cluster', data.data, 60000);
        updateClusterStats(data.data);
    });

    eventSource.addEventListener('topics', function(e) {
        const data = JSON.parse(e.data);
        // Cache topics list for faster page loads
        StreamlineCache.set('api:topics', data.data, 60000);
        updateTopicsTable(data.data);
    });

    eventSource.addEventListener('consumer_groups', function(e) {
        const data = JSON.parse(e.data);
        // Cache consumer groups for faster page loads
        StreamlineCache.set('api:consumer-groups', data.data, 60000);
        updateConsumerGroupsTable(data.data);
    });

    eventSource.addEventListener('ping', function(e) {
        console.log('SSE ping received');
    });
}

function updateClusterStats(data) {
    const messagesEl = document.getElementById('messages-per-sec');
    if (messagesEl) {
        messagesEl.textContent = data.messages_per_second.toFixed(1);
    }

    const bytesInEl = document.getElementById('bytes-in');
    if (bytesInEl) {
        bytesInEl.textContent = formatBytesPerSec(data.bytes_in_per_second);
    }

    const bytesOutEl = document.getElementById('bytes-out');
    if (bytesOutEl) {
        bytesOutEl.textContent = formatBytesPerSec(data.bytes_out_per_second);
    }

    // Update sparklines with new data points
    addSparklinePoint('messages', data.messages_per_second);
    addSparklinePoint('throughput', data.bytes_in_per_second);
}

function updateTopicsTable(topics) {
    const table = document.getElementById('topics-table');
    if (!table) return;
    // Real-time update logic would go here
}

function updateConsumerGroupsTable(groups) {
    const table = document.getElementById('consumer-groups-table');
    if (!table) return;
    // Real-time update logic would go here
}

function formatBytesPerSec(bytes) {
    const KB = 1024;
    const MB = KB * 1024;
    const GB = MB * 1024;

    if (bytes >= GB) {
        return (bytes / GB).toFixed(2) + ' GB/s';
    } else if (bytes >= MB) {
        return (bytes / MB).toFixed(2) + ' MB/s';
    } else if (bytes >= KB) {
        return (bytes / KB).toFixed(2) + ' KB/s';
    } else {
        return bytes.toFixed(0) + ' B/s';
    }
}

// Modal functions
function showCreateTopicModal() {
    document.getElementById('create-topic-modal').classList.remove('hidden');
}

function hideModal(id) {
    document.getElementById(id).classList.add('hidden');
}

async function createTopic(event) {
    event.preventDefault();

    const form = event.target;
    const data = {
        name: form.name.value,
        partitions: parseInt(form.partitions.value),
        replication_factor: parseInt(form.replication_factor.value)
    };

    try {
        const response = await fetch('/api/topics', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (response.ok) {
            // Invalidate cache on mutation
            StreamlineCache.invalidate(/^api:topics/);
            StreamlineCache.invalidate('api:cluster');
            hideModal('create-topic-modal');
            window.location.reload();
        } else {
            const error = await response.text();
            alert('Failed to create topic: ' + error);
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

async function deleteTopic(name) {
    if (!confirm('Are you sure you want to delete topic "' + name + '"?')) {
        return;
    }

    try {
        const response = await fetch(`/api/topics/${encodeURIComponent(name)}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            // Invalidate cache on mutation
            StreamlineCache.invalidate(/^api:topics/);
            StreamlineCache.invalidate('api:cluster');
            window.location.href = '/topics';
        } else {
            const error = await response.text();
            alert('Failed to delete topic: ' + error);
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

// Load metrics on metrics page
async function loadMetrics() {
    const output = document.getElementById('metrics-output');
    if (!output) return;

    try {
        const response = await fetch('/api/metrics');
        const text = await response.text();
        output.textContent = text;
    } catch (e) {
        output.textContent = 'Error loading metrics: ' + e.message;
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    // Load saved preferences
    loadTheme();
    loadAutoRefreshPreference();

    // Connect SSE only if auto-refresh is enabled
    if (autoRefreshEnabled) {
        connectSSE();
    } else {
        updateConnectionStatus('disconnected', 'Paused');
    }

    // Register keyboard shortcuts
    document.addEventListener('keydown', handleKeyboardShortcuts);

    // Load metrics if on metrics page
    if (document.getElementById('metrics-output')) {
        loadMetrics();
        if (autoRefreshEnabled) {
            setInterval(loadMetrics, 10000);
        }
    }

    // Initialize pagination for tables with many rows
    ['topics-table', 'consumer-groups-table', 'brokers-table'].forEach(tableId => {
        const table = document.getElementById(tableId);
        if (table && table.querySelectorAll('tr:not(.empty-state-row)').length > 10) {
            initPagination(tableId, 10);
        }
    });

    // Initialize favorite buttons
    updateFavoriteButtons();

    // Add blur handler to restore shortcut hints
    const searchInputs = document.querySelectorAll('.search-input');
    searchInputs.forEach(input => {
        input.addEventListener('blur', function() {
            const shortcutHint = this.parentElement.querySelector('.search-shortcut');
            if (shortcutHint) {
                shortcutHint.style.display = '';
            }
        });
        input.addEventListener('focus', function() {
            const shortcutHint = this.parentElement.querySelector('.search-shortcut');
            if (shortcutHint) {
                shortcutHint.style.display = 'none';
            }
        });
    });

    // Close shortcuts modal when clicking outside
    const shortcutsModal = document.getElementById('shortcuts-modal');
    if (shortcutsModal) {
        shortcutsModal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.classList.add('hidden');
            }
        });
    }

    // Heatmap cell click handlers
    const heatmapTable = document.getElementById('heatmap-table');
    if (heatmapTable) {
        heatmapTable.addEventListener('click', function(e) {
            const cell = e.target.closest('.heatmap-cell');
            if (!cell || cell.classList.contains('heatmap-empty')) return;

            const group = cell.dataset.group;
            const topic = cell.dataset.topic;
            const lag = cell.dataset.lag;
            const partitions = cell.dataset.partitions;

            // Update details panel
            const detailsPanel = document.getElementById('heatmap-details');
            if (detailsPanel) {
                const severityClass = Array.from(cell.classList).find(c => c.startsWith('severity-'));
                const severity = severityClass ? severityClass.replace('severity-', '').charAt(0).toUpperCase() + severityClass.replace('severity-', '').slice(1) : 'Unknown';

                detailsPanel.innerHTML = `
                    <div class="heatmap-detail-grid">
                        <div class="heatmap-detail-item">
                            <div class="label">Consumer Group</div>
                            <div class="value">${group}</div>
                        </div>
                        <div class="heatmap-detail-item">
                            <div class="label">Topic</div>
                            <div class="value">${topic}</div>
                        </div>
                        <div class="heatmap-detail-item">
                            <div class="label">Total Lag</div>
                            <div class="value">${Number(lag).toLocaleString()}</div>
                        </div>
                        <div class="heatmap-detail-item">
                            <div class="label">Partitions</div>
                            <div class="value">${partitions}</div>
                        </div>
                        <div class="heatmap-detail-item">
                            <div class="label">Severity</div>
                            <div class="value"><span class="legend-color ${severityClass}" style="display: inline-block; width: 12px; height: 12px; margin-right: 6px;"></span>${severity}</div>
                        </div>
                    </div>
                    <div style="margin-top: 1rem;">
                        <a href="/consumer-groups/${encodeURIComponent(group)}" class="btn btn-sm">View Consumer Group</a>
                        <a href="/topics/${encodeURIComponent(topic)}" class="btn btn-sm">View Topic</a>
                    </div>
                `;
            }

            // Highlight selected cell
            document.querySelectorAll('.heatmap-cell.selected').forEach(c => c.classList.remove('selected'));
            cell.classList.add('selected');
        });
    }
});

// Message Producer functions
async function produceMessage(event, topicName) {
    event.preventDefault();

    const form = event.target;
    const partition = form.partition.value ? parseInt(form.partition.value) : null;
    const key = form.key.value || null;
    const value = form.value.value;

    // Collect headers
    const headers = {};
    const headerKeys = form.querySelectorAll('input[name="header-key[]"]');
    const headerValues = form.querySelectorAll('input[name="header-value[]"]');
    headerKeys.forEach((keyInput, i) => {
        if (keyInput.value && headerValues[i].value) {
            headers[keyInput.value] = headerValues[i].value;
        }
    });

    const request = {
        key: key,
        value: value,
        partition: partition,
        headers: headers
    };

    try {
        const response = await fetch(`/api/topics/${encodeURIComponent(topicName)}/messages`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });

        const resultsDiv = document.getElementById('produce-results');
        const resultsList = document.getElementById('results-list');

        if (response.ok) {
            const data = await response.json();
            const result = data.offsets[0];

            showToast(`Message produced successfully to partition ${result.partition} at offset ${result.offset}`, 'success');

            // Add to results list
            resultsDiv.style.display = 'block';
            const resultItem = document.createElement('div');
            resultItem.className = 'result-item';
            resultItem.innerHTML = `
                <span class="success">Partition ${result.partition}, Offset ${result.offset}</span>
                <span>${new Date().toLocaleTimeString()}</span>
            `;
            resultsList.insertBefore(resultItem, resultsList.firstChild);

            // Clear the form value but keep key
            form.value.value = '';

            // Invalidate topic cache
            StreamlineCache.invalidate(`api:/api/topics/${topicName}`);
        } else {
            const error = await response.text();
            showToast(`Failed to produce message: ${error}`, 'error');
        }
    } catch (error) {
        showToast(`Error: ${error.message}`, 'error');
    }
}

function addHeaderRow() {
    const container = document.getElementById('headers-container');
    const row = document.createElement('div');
    row.className = 'header-row';
    row.innerHTML = `
        <input type="text" name="header-key[]" placeholder="Header key">
        <input type="text" name="header-value[]" placeholder="Header value">
        <button type="button" class="btn btn-small" onclick="removeHeaderRow(this)">Remove</button>
    `;
    container.appendChild(row);
}

function removeHeaderRow(button) {
    const row = button.closest('.header-row');
    const container = document.getElementById('headers-container');
    // Keep at least one row
    if (container.children.length > 1) {
        row.remove();
    } else {
        // Clear the inputs instead of removing
        row.querySelectorAll('input').forEach(input => input.value = '');
    }
}

// JSON validation for producer
document.addEventListener('DOMContentLoaded', function() {
    const valueInput = document.getElementById('value');
    const validationMsg = document.getElementById('json-validation');

    if (valueInput && validationMsg) {
        valueInput.addEventListener('input', function() {
            const value = valueInput.value.trim();
            if (!value) {
                validationMsg.textContent = '';
                validationMsg.className = 'validation-message';
                return;
            }

            try {
                JSON.parse(value);
                validationMsg.textContent = 'Valid JSON';
                validationMsg.className = 'validation-message valid';
            } catch (e) {
                validationMsg.textContent = 'Plain text (not JSON)';
                validationMsg.className = 'validation-message';
            }
        });
    }
});

// Offset Management Functions
function updateStrategyOptions() {
    const strategy = document.getElementById('reset-strategy').value;
    const offsetGroup = document.getElementById('specific-offset-group');
    if (offsetGroup) {
        offsetGroup.style.display = strategy === 'offset' ? 'block' : 'none';
    }
}

function toggleAllPartitions() {
    const selectAll = document.getElementById('select-all');
    const checkboxes = document.querySelectorAll('.partition-checkbox');
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

function getSelectedPartitions() {
    const selected = [];
    document.querySelectorAll('.partition-checkbox:checked').forEach(cb => {
        selected.push({
            topic: cb.dataset.topic,
            partition: parseInt(cb.dataset.partition)
        });
    });
    return selected;
}

async function dryRunReset(groupId) {
    const topic = document.getElementById('reset-topic').value;
    if (!topic) {
        showToast('Please select a topic', 'warning');
        return;
    }

    const strategy = document.getElementById('reset-strategy').value;
    const request = {
        topic: topic,
        strategy: strategy
    };

    if (strategy === 'offset') {
        const offset = parseInt(document.getElementById('specific-offset').value);
        if (isNaN(offset)) {
            showToast('Please enter a valid offset', 'warning');
            return;
        }
        request.offset = offset;
    }

    // Get selected partitions if any
    const selected = getSelectedPartitions().filter(p => p.topic === topic);
    if (selected.length > 0) {
        request.partitions = selected.map(p => p.partition);
    }

    try {
        const response = await fetch(`/api/consumer-groups/${groupId}/reset-offsets/dry-run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });

        if (!response.ok) {
            const error = await response.text();
            showToast(`Preview failed: ${error}`, 'error');
            return;
        }

        const result = await response.json();
        displayPreviewResults(result);
    } catch (e) {
        showToast(`Preview failed: ${e.message}`, 'error');
    }
}

function displayPreviewResults(result) {
    const container = document.getElementById('preview-results');
    const content = document.getElementById('preview-content');

    if (!container || !content) return;

    let html = `
        <div class="preview-summary">
            <p><strong>Strategy:</strong> ${result.strategy}</p>
            <p><strong>Topic:</strong> ${result.topic}</p>
            <p><strong>Partitions:</strong> ${result.partitions.length}</p>
        </div>
        <table class="data-table">
            <thead>
                <tr>
                    <th>Partition</th>
                    <th>Previous Offset</th>
                    <th>New Offset</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
    `;

    result.partitions.forEach(p => {
        const change = p.new_offset - p.previous_offset;
        const changeClass = change > 0 ? 'success' : change < 0 ? 'warning' : '';
        const changeText = change > 0 ? `+${change}` : change.toString();
        html += `
            <tr>
                <td>${p.partition}</td>
                <td>${p.previous_offset}</td>
                <td>${p.new_offset} <span class="${changeClass}">(${changeText})</span></td>
                <td>${p.error ? `<span class="error">${p.error}</span>` : '<span class="success">OK</span>'}</td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    content.innerHTML = html;
    container.style.display = 'block';
}

async function executeReset(groupId) {
    const topic = document.getElementById('reset-topic').value;
    if (!topic) {
        showToast('Please select a topic', 'warning');
        return;
    }

    const strategy = document.getElementById('reset-strategy').value;
    const request = {
        topic: topic,
        strategy: strategy
    };

    if (strategy === 'offset') {
        const offset = parseInt(document.getElementById('specific-offset').value);
        if (isNaN(offset)) {
            showToast('Please enter a valid offset', 'warning');
            return;
        }
        request.offset = offset;
    }

    // Get selected partitions if any
    const selected = getSelectedPartitions().filter(p => p.topic === topic);
    if (selected.length > 0) {
        request.partitions = selected.map(p => p.partition);
    }

    // Confirm before executing
    if (!confirm(`Are you sure you want to reset offsets for topic "${topic}" using strategy "${strategy}"? This action cannot be undone.`)) {
        return;
    }

    try {
        const response = await fetch(`/api/consumer-groups/${groupId}/reset-offsets`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });

        if (!response.ok) {
            const error = await response.text();
            showToast(`Reset failed: ${error}`, 'error');
            return;
        }

        const result = await response.json();
        showToast(`Successfully reset ${result.success_count} partition(s)`, 'success');

        // Refresh the page after a short delay
        setTimeout(() => window.location.reload(), 1500);
    } catch (e) {
        showToast(`Reset failed: ${e.message}`, 'error');
    }
}

// Log Viewer Functions
let logEventSource = null;
let logStats = { total: 0, errors: 0, warnings: 0, buffer: 0 };
const LOG_MAX_ENTRIES = 1000;

function initLogViewer() {
    if (!document.getElementById('log-output')) return;
    startLogStreaming();
}

function startLogStreaming() {
    const level = document.getElementById('log-level-filter')?.value || 'INFO';
    const url = `/api/v1/logs/stream?level=${level}`;

    if (logEventSource) {
        logEventSource.close();
    }

    logEventSource = new EventSource(url);

    logEventSource.addEventListener('connected', function(e) {
        const output = document.getElementById('log-output');
        if (output) {
            const loading = output.querySelector('.log-loading');
            if (loading) loading.remove();
        }
    });

    logEventSource.addEventListener('logs', function(e) {
        try {
            const entries = JSON.parse(e.data);
            appendLogEntries(entries);
        } catch (err) {
            console.error('Failed to parse log entries:', err);
        }
    });

    logEventSource.onerror = function() {
        const output = document.getElementById('log-output');
        if (output && logEventSource.readyState === EventSource.CLOSED) {
            output.innerHTML = '<div class="log-loading">Connection lost. Reconnecting...</div>';
            setTimeout(startLogStreaming, 3000);
        }
    };
}

function appendLogEntries(entries) {
    const output = document.getElementById('log-output');
    if (!output) return;

    const autoScroll = document.getElementById('auto-scroll')?.checked ?? true;
    const wasAtBottom = output.scrollHeight - output.scrollTop <= output.clientHeight + 50;

    entries.forEach(entry => {
        const div = document.createElement('div');
        div.className = 'log-entry';
        div.dataset.level = entry.level;
        div.dataset.target = entry.target;
        div.dataset.message = entry.message.toLowerCase();

        const timestamp = formatLogTimestamp(entry.timestamp);
        const levelClass = `log-level-${entry.level.toLowerCase()}`;

        div.innerHTML = `
            <span class="log-timestamp">${timestamp}</span>
            <span class="log-level ${levelClass}">${entry.level}</span>
            <span class="log-target" title="${entry.target}">${entry.target}</span>
            <span class="log-message">${escapeHtml(entry.message)}</span>
        `;

        output.appendChild(div);

        // Update stats
        logStats.total++;
        if (entry.level === 'ERROR') logStats.errors++;
        if (entry.level === 'WARN') logStats.warnings++;
    });

    // Trim old entries
    while (output.children.length > LOG_MAX_ENTRIES) {
        output.removeChild(output.firstChild);
    }

    // Apply current filter
    filterLogs();

    // Update stats display
    updateLogStats();

    // Auto-scroll if enabled
    if (autoScroll && wasAtBottom) {
        output.scrollTop = output.scrollHeight;
    }
}

function formatLogTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

function filterLogs() {
    const output = document.getElementById('log-output');
    if (!output) return;

    const levelFilter = document.getElementById('log-level-filter')?.value || '';
    const searchFilter = (document.getElementById('log-search')?.value || '').toLowerCase();

    const levelPriority = { 'TRACE': 0, 'DEBUG': 1, 'INFO': 2, 'WARN': 3, 'ERROR': 4 };
    const minLevel = levelPriority[levelFilter] || 0;

    output.querySelectorAll('.log-entry').forEach(entry => {
        const entryLevel = levelPriority[entry.dataset.level] || 0;
        const matchesLevel = entryLevel >= minLevel;
        const matchesSearch = !searchFilter ||
            entry.dataset.message.includes(searchFilter) ||
            entry.dataset.target.toLowerCase().includes(searchFilter);

        entry.classList.toggle('hidden', !matchesLevel || !matchesSearch);
    });
}

function toggleStreaming() {
    const streaming = document.getElementById('log-streaming')?.checked;

    if (streaming) {
        startLogStreaming();
    } else if (logEventSource) {
        logEventSource.close();
        logEventSource = null;
    }
}

function clearLogDisplay() {
    const output = document.getElementById('log-output');
    if (output) {
        output.innerHTML = '';
    }
    logStats = { total: 0, errors: 0, warnings: 0, buffer: 0 };
    updateLogStats();
}

function updateLogStats() {
    const statTotal = document.getElementById('stat-total');
    const statErrors = document.getElementById('stat-errors');
    const statWarnings = document.getElementById('stat-warnings');
    const statBuffer = document.getElementById('stat-buffer');

    if (statTotal) statTotal.textContent = logStats.total;
    if (statErrors) statErrors.textContent = logStats.errors;
    if (statWarnings) statWarnings.textContent = logStats.warnings;
    if (statBuffer) {
        const output = document.getElementById('log-output');
        statBuffer.textContent = output ? output.children.length : 0;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize log viewer on page load
document.addEventListener('DOMContentLoaded', initLogViewer);

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (logEventSource) {
        logEventSource.close();
    }
});
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_templates_new() {
        let _templates = Templates::new();
        // Just verify we can create the template renderer
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1.00K");
        assert_eq!(format_number(1000000), "1.00M");
        assert_eq!(format_number(1000000000), "1.00B");
    }

    #[test]
    fn test_dashboard_render() {
        let templates = Templates::new();
        let overview = ClusterOverview::default();
        let html = templates.dashboard(&overview);

        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Dashboard"));
        assert!(html.contains("Streamline"));
    }

    #[test]
    fn test_topics_list_render() {
        let templates = Templates::new();
        let topics = vec![TopicInfo {
            name: "test-topic".to_string(),
            partition_count: 3,
            replication_factor: 1,
            is_internal: false,
            total_messages: 1000,
            total_bytes: 10240,
        }];
        let html = templates.topics_list(&topics);

        assert!(html.contains("test-topic"));
        assert!(html.contains("Topics"));
    }

    #[test]
    fn test_error_page_render() {
        let templates = Templates::new();
        let html = templates.error_page("Not Found", "The page was not found");

        assert!(html.contains("Not Found"));
        assert!(html.contains("The page was not found"));
    }
}
