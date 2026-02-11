//! Interactive TUI dashboard for Streamline
//!
//! Provides a real-time terminal dashboard similar to `top` or `htop`
//! for monitoring topics, partitions, consumer groups, and performance.
//!
//! Features:
//! - Live message streaming
//! - Sparkline throughput charts
//! - Topic/group detail panels
//! - Search and filtering
//! - Sorting by columns
//! - Alerts and gauges
//! - Mouse support
//! - Color themes

use crate::cli_utils::tui_state::{SortConfig, SortPreferences, TuiPersistentState};
use crate::{GroupCoordinator, TopicManager};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseButton,
        MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Clear, Gauge, List, ListItem, Paragraph, Row, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Sparkline, Table, Tabs, Wrap,
    },
    Frame, Terminal,
};
use std::collections::VecDeque;
use std::io::{self, Write as IoWrite};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Dashboard configuration
pub struct DashboardConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// Refresh interval
    pub refresh_interval: Duration,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            refresh_interval: Duration::from_secs(1),
        }
    }
}

/// Color theme
#[derive(Clone, Copy, PartialEq)]
enum Theme {
    Dark,
    Light,
}

impl Theme {
    fn toggle(&self) -> Self {
        match self {
            Theme::Dark => Theme::Light,
            Theme::Light => Theme::Dark,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Theme::Dark => "dark",
            Theme::Light => "light",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "light" => Theme::Light,
            _ => Theme::Dark,
        }
    }

    fn bg(&self) -> Color {
        match self {
            Theme::Dark => Color::Reset,
            Theme::Light => Color::White,
        }
    }

    fn fg(&self) -> Color {
        match self {
            Theme::Dark => Color::White,
            Theme::Light => Color::Black,
        }
    }

    fn highlight(&self) -> Color {
        match self {
            Theme::Dark => Color::Yellow,
            Theme::Light => Color::Blue,
        }
    }

    fn accent(&self) -> Color {
        match self {
            Theme::Dark => Color::Cyan,
            Theme::Light => Color::DarkGray,
        }
    }

    fn success(&self) -> Color {
        Color::Green
    }

    fn warning(&self) -> Color {
        Color::Yellow
    }

    fn error(&self) -> Color {
        Color::Red
    }

    fn muted(&self) -> Color {
        match self {
            Theme::Dark => Color::DarkGray,
            Theme::Light => Color::Gray,
        }
    }
}

/// Sort column for topics
#[derive(Clone, Copy, PartialEq)]
enum TopicSortColumn {
    Name,
    Partitions,
    Messages,
    Size,
    Throughput,
}

impl TopicSortColumn {
    fn next(&self) -> Self {
        match self {
            Self::Name => Self::Partitions,
            Self::Partitions => Self::Messages,
            Self::Messages => Self::Size,
            Self::Size => Self::Throughput,
            Self::Throughput => Self::Name,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Partitions => "partitions",
            Self::Messages => "messages",
            Self::Size => "size",
            Self::Throughput => "throughput",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "partitions" => Self::Partitions,
            "messages" => Self::Messages,
            "size" => Self::Size,
            "throughput" => Self::Throughput,
            _ => Self::Name,
        }
    }
}

/// Sort column for groups
#[derive(Clone, Copy, PartialEq)]
enum GroupSortColumn {
    Id,
    State,
    Members,
    Lag,
}

impl GroupSortColumn {
    fn next(&self) -> Self {
        match self {
            Self::Id => Self::State,
            Self::State => Self::Members,
            Self::Members => Self::Lag,
            Self::Lag => Self::Id,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::State => "state",
            Self::Members => "members",
            Self::Lag => "lag",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "state" => Self::State,
            "members" => Self::Members,
            "lag" => Self::Lag,
            _ => Self::Id,
        }
    }
}

/// View mode
#[derive(Clone, Copy, PartialEq)]
enum ViewMode {
    Normal,
    Search,
    Command,
    TopicDetail,
    GroupDetail,
    LiveMessages,
    Help,
}

/// Alert severity
#[derive(Clone, Copy, PartialEq)]
enum AlertSeverity {
    Info,
    Warning,
    Error,
}

/// Alert message
#[derive(Clone)]
struct Alert {
    severity: AlertSeverity,
    message: String,
    timestamp: Instant,
}

/// Dashboard state
struct DashboardState {
    /// Currently selected tab
    tab_index: usize,
    /// Tab titles
    tabs: Vec<&'static str>,
    /// Topic list state
    topic_selected: usize,
    /// Topic scroll offset
    topic_scroll: usize,
    /// Group list state
    group_selected: usize,
    /// Group scroll offset
    group_scroll: usize,
    /// Cached topic data
    topics: Vec<TopicInfo>,
    /// Cached group data
    groups: Vec<GroupInfo>,
    /// Server stats
    stats: ServerStats,
    /// Last refresh time
    last_refresh: Instant,
    /// Should quit
    should_quit: bool,
    /// Current view mode
    view_mode: ViewMode,
    /// Search/filter query
    search_query: String,
    /// Command input
    command_input: String,
    /// Current theme
    theme: Theme,
    /// Topic sort column
    topic_sort: TopicSortColumn,
    /// Topic sort ascending
    topic_sort_asc: bool,
    /// Group sort column
    group_sort: GroupSortColumn,
    /// Group sort ascending
    group_sort_asc: bool,
    /// Throughput history for sparkline (messages per second)
    throughput_history: VecDeque<u64>,
    /// Previous message count for throughput calculation
    prev_messages: i64,
    /// Previous refresh time for throughput calculation
    prev_refresh: Instant,
    /// Current throughput
    current_throughput: f64,
    /// Active alerts
    alerts: Vec<Alert>,
    /// Bookmarked topics
    bookmarked_topics: Vec<String>,
    /// Bookmarked groups
    bookmarked_groups: Vec<String>,
    /// Live messages buffer
    live_messages: VecDeque<LiveMessage>,
    /// Live message scroll
    live_message_scroll: usize,
    /// Selected topic for detail view
    detail_topic: Option<TopicInfo>,
    /// Selected group for detail view
    detail_group: Option<GroupInfo>,
    /// Partition details for selected topic
    partition_details: Vec<PartitionInfo>,
    /// Server start time (for uptime)
    server_start: Instant,
    /// Mouse enabled
    mouse_enabled: bool,
}

#[derive(Clone)]
struct TopicInfo {
    name: String,
    partitions: i32,
    total_messages: i64,
    size_bytes: u64,
    messages_per_sec: f64,
    bookmarked: bool,
}

#[derive(Clone)]
struct PartitionInfo {
    partition: i32,
    earliest_offset: i64,
    latest_offset: i64,
    messages: i64,
    size_bytes: u64,
}

#[derive(Clone)]
struct GroupInfo {
    id: String,
    state: String,
    members: usize,
    topics: Vec<String>,
    total_lag: i64,
    bookmarked: bool,
}

#[derive(Clone)]
struct LiveMessage {
    topic: String,
    partition: i32,
    offset: i64,
    #[allow(dead_code)] // Reserved for displaying message keys in live view
    key: Option<String>,
    value_preview: String,
    timestamp: Instant,
}

#[derive(Default, Clone)]
struct ServerStats {
    topics_count: usize,
    partitions_count: usize,
    groups_count: usize,
    messages_total: i64,
    storage_bytes: u64,
    uptime_secs: u64,
}

impl DashboardState {
    fn new() -> Self {
        let now = Instant::now();

        // Load persistent state from disk
        let persistent = TuiPersistentState::load();

        // Parse sort preferences
        let (topic_sort, topic_sort_asc) = persistent
            .sort_preferences
            .topics
            .as_ref()
            .map(|s| (TopicSortColumn::from_str(&s.column), s.ascending))
            .unwrap_or((TopicSortColumn::Name, true));

        let (group_sort, group_sort_asc) = persistent
            .sort_preferences
            .groups
            .as_ref()
            .map(|s| (GroupSortColumn::from_str(&s.column), s.ascending))
            .unwrap_or((GroupSortColumn::Id, true));

        Self {
            tab_index: persistent.last_tab.min(4), // Clamp to valid tab range
            tabs: vec!["Overview", "Topics", "Groups", "Performance", "Alerts"],
            topic_selected: 0,
            topic_scroll: 0,
            group_selected: 0,
            group_scroll: 0,
            topics: vec![],
            groups: vec![],
            stats: ServerStats::default(),
            last_refresh: now,
            should_quit: false,
            view_mode: ViewMode::Normal,
            search_query: String::new(),
            command_input: String::new(),
            theme: Theme::from_str(&persistent.theme),
            topic_sort,
            topic_sort_asc,
            group_sort,
            group_sort_asc,
            throughput_history: VecDeque::with_capacity(60),
            prev_messages: 0,
            prev_refresh: now,
            current_throughput: 0.0,
            alerts: vec![],
            bookmarked_topics: persistent.bookmarked_topics.into_iter().collect(),
            bookmarked_groups: persistent.bookmarked_groups.into_iter().collect(),
            live_messages: VecDeque::with_capacity(100),
            live_message_scroll: 0,
            detail_topic: None,
            detail_group: None,
            partition_details: vec![],
            server_start: now,
            mouse_enabled: true,
        }
    }

    /// Convert current state to persistent format for saving
    fn to_persistent_state(&self) -> TuiPersistentState {
        TuiPersistentState {
            version: 1,
            bookmarked_topics: self.bookmarked_topics.iter().cloned().collect(),
            bookmarked_groups: self.bookmarked_groups.iter().cloned().collect(),
            theme: self.theme.as_str().to_string(),
            last_tab: self.tab_index,
            sort_preferences: SortPreferences {
                topics: Some(SortConfig {
                    column: self.topic_sort.as_str().to_string(),
                    ascending: self.topic_sort_asc,
                }),
                groups: Some(SortConfig {
                    column: self.group_sort.as_str().to_string(),
                    ascending: self.group_sort_asc,
                }),
            },
        }
    }

    /// Save current state to disk (silently ignores errors)
    fn save_state(&self) {
        let _ = self.to_persistent_state().save();
    }

    fn next_tab(&mut self) {
        self.tab_index = (self.tab_index + 1) % self.tabs.len();
    }

    fn prev_tab(&mut self) {
        if self.tab_index > 0 {
            self.tab_index -= 1;
        } else {
            self.tab_index = self.tabs.len() - 1;
        }
    }

    fn filtered_topics(&self) -> Vec<&TopicInfo> {
        let mut topics: Vec<&TopicInfo> = self
            .topics
            .iter()
            .filter(|t| {
                self.search_query.is_empty()
                    || t.name
                        .to_lowercase()
                        .contains(&self.search_query.to_lowercase())
            })
            .collect();

        // Sort
        topics.sort_by(|a, b| {
            let cmp = match self.topic_sort {
                TopicSortColumn::Name => a.name.cmp(&b.name),
                TopicSortColumn::Partitions => a.partitions.cmp(&b.partitions),
                TopicSortColumn::Messages => a.total_messages.cmp(&b.total_messages),
                TopicSortColumn::Size => a.size_bytes.cmp(&b.size_bytes),
                TopicSortColumn::Throughput => a
                    .messages_per_sec
                    .partial_cmp(&b.messages_per_sec)
                    .unwrap_or(std::cmp::Ordering::Equal),
            };
            if self.topic_sort_asc {
                cmp
            } else {
                cmp.reverse()
            }
        });

        topics
    }

    fn filtered_groups(&self) -> Vec<&GroupInfo> {
        let mut groups: Vec<&GroupInfo> = self
            .groups
            .iter()
            .filter(|g| {
                self.search_query.is_empty()
                    || g.id
                        .to_lowercase()
                        .contains(&self.search_query.to_lowercase())
            })
            .collect();

        // Sort
        groups.sort_by(|a, b| {
            let cmp = match self.group_sort {
                GroupSortColumn::Id => a.id.cmp(&b.id),
                GroupSortColumn::State => a.state.cmp(&b.state),
                GroupSortColumn::Members => a.members.cmp(&b.members),
                GroupSortColumn::Lag => a.total_lag.cmp(&b.total_lag),
            };
            if self.group_sort_asc {
                cmp
            } else {
                cmp.reverse()
            }
        });

        groups
    }

    fn toggle_topic_bookmark(&mut self) {
        let topics = self.filtered_topics();
        if let Some(topic) = topics.get(self.topic_selected) {
            let name = topic.name.clone();
            if self.bookmarked_topics.contains(&name) {
                self.bookmarked_topics.retain(|t| t != &name);
            } else {
                self.bookmarked_topics.push(name);
            }
            // Update the bookmarked flag
            for t in &mut self.topics {
                t.bookmarked = self.bookmarked_topics.contains(&t.name);
            }
            // Persist bookmark change
            self.save_state();
        }
    }

    fn toggle_group_bookmark(&mut self) {
        let groups = self.filtered_groups();
        if let Some(group) = groups.get(self.group_selected) {
            let id = group.id.clone();
            if self.bookmarked_groups.contains(&id) {
                self.bookmarked_groups.retain(|g| g != &id);
            } else {
                self.bookmarked_groups.push(id);
            }
            // Update the bookmarked flag
            for g in &mut self.groups {
                g.bookmarked = self.bookmarked_groups.contains(&g.id);
            }
            // Persist bookmark change
            self.save_state();
        }
    }

    fn add_alert(&mut self, severity: AlertSeverity, message: String) {
        self.alerts.push(Alert {
            severity,
            message,
            timestamp: Instant::now(),
        });
        // Keep only last 50 alerts
        while self.alerts.len() > 50 {
            self.alerts.remove(0);
        }
    }

    fn check_alerts(&mut self) {
        // Collect alerts to add (avoids borrow conflict)
        let mut new_alerts: Vec<(AlertSeverity, String)> = Vec::new();

        // Check for high lag
        for group in &self.groups {
            if group.total_lag > 100_000 {
                let msg = format!(
                    "Critical lag on group '{}': {} messages behind",
                    group.id, group.total_lag
                );
                if !self.alerts.iter().any(|a| a.message == msg) {
                    new_alerts.push((AlertSeverity::Error, msg));
                }
            } else if group.total_lag > 10_000 {
                let msg = format!(
                    "High lag on group '{}': {} messages behind",
                    group.id, group.total_lag
                );
                if !self.alerts.iter().any(|a| a.message == msg) {
                    new_alerts.push((AlertSeverity::Warning, msg));
                }
            }
        }

        // Check storage (if we had real data)
        if self.stats.storage_bytes > 10_000_000_000 {
            let msg = format!(
                "Storage usage high: {}",
                format_bytes(self.stats.storage_bytes)
            );
            if !self.alerts.iter().any(|a| a.message == msg) {
                new_alerts.push((AlertSeverity::Warning, msg));
            }
        }

        // Now add all collected alerts
        for (severity, msg) in new_alerts {
            self.add_alert(severity, msg);
        }
    }

    fn execute_command(&mut self, data_dir: &PathBuf) -> Option<String> {
        let parts: Vec<&str> = self.command_input.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "q" | "quit" | "exit" => {
                self.should_quit = true;
                None
            }
            "refresh" | "r" => {
                self.refresh(data_dir);
                Some("Refreshed".to_string())
            }
            "theme" => {
                self.theme = self.theme.toggle();
                Some(format!(
                    "Theme: {:?}",
                    if self.theme == Theme::Dark {
                        "Dark"
                    } else {
                        "Light"
                    }
                ))
            }
            "clear" => {
                self.alerts.clear();
                Some("Alerts cleared".to_string())
            }
            "help" => {
                self.view_mode = ViewMode::Help;
                None
            }
            "export" => {
                if parts.len() > 1 {
                    match parts[1] {
                        "topics" => Some(self.export_topics()),
                        "groups" => Some(self.export_groups()),
                        _ => Some("Usage: export topics|groups".to_string()),
                    }
                } else {
                    Some("Usage: export topics|groups".to_string())
                }
            }
            _ => Some(format!("Unknown command: {}", parts[0])),
        }
    }

    fn export_topics(&self) -> String {
        let json = serde_json::json!({
            "topics": self.topics.iter().map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "partitions": t.partitions,
                    "messages": t.total_messages,
                    "size_bytes": t.size_bytes,
                    "throughput": t.messages_per_sec,
                })
            }).collect::<Vec<_>>()
        });

        let filename = format!(
            "streamline_topics_{}.json",
            chrono::Local::now().format("%Y%m%d_%H%M%S")
        );
        if let Ok(mut file) = std::fs::File::create(&filename) {
            if file
                .write_all(
                    serde_json::to_string_pretty(&json)
                        .unwrap_or_default()
                        .as_bytes(),
                )
                .is_ok()
            {
                return format!("Exported to {}", filename);
            }
        }
        "Export failed".to_string()
    }

    fn export_groups(&self) -> String {
        let json = serde_json::json!({
            "groups": self.groups.iter().map(|g| {
                serde_json::json!({
                    "id": g.id,
                    "state": g.state,
                    "members": g.members,
                    "topics": g.topics,
                    "lag": g.total_lag,
                })
            }).collect::<Vec<_>>()
        });

        let filename = format!(
            "streamline_groups_{}.json",
            chrono::Local::now().format("%Y%m%d_%H%M%S")
        );
        if let Ok(mut file) = std::fs::File::create(&filename) {
            if file
                .write_all(
                    serde_json::to_string_pretty(&json)
                        .unwrap_or_default()
                        .as_bytes(),
                )
                .is_ok()
            {
                return format!("Exported to {}", filename);
            }
        }
        "Export failed".to_string()
    }

    fn refresh(&mut self, data_dir: &PathBuf) {
        use std::sync::Arc;

        // Calculate throughput
        let elapsed = self.prev_refresh.elapsed().as_secs_f64();

        // Refresh topics
        let manager = match TopicManager::new(data_dir) {
            Ok(m) => Arc::new(m),
            Err(_) => {
                self.last_refresh = Instant::now();
                return;
            }
        };

        if let Ok(topic_list) = manager.list_topics() {
            let mut total_messages = 0i64;
            let mut total_partitions = 0i32;
            let mut total_storage = 0u64;

            self.topics = topic_list
                .iter()
                .map(|t| {
                    let mut msgs = 0i64;
                    let mut size = 0u64;

                    for p in 0..t.num_partitions {
                        if let Ok(latest) = manager.latest_offset(&t.name, p) {
                            if let Ok(earliest) = manager.earliest_offset(&t.name, p) {
                                msgs += latest - earliest;
                            }
                        }
                        // Estimate size (actual implementation would read from storage)
                        // Assume average 500 bytes per message
                        size += (msgs as u64) * 500;
                    }
                    total_messages += msgs;
                    total_partitions += t.num_partitions;
                    total_storage += size;

                    TopicInfo {
                        name: t.name.clone(),
                        partitions: t.num_partitions,
                        total_messages: msgs,
                        size_bytes: size,
                        messages_per_sec: 0.0, // Will be calculated below
                        bookmarked: self.bookmarked_topics.contains(&t.name),
                    }
                })
                .collect();

            // Calculate throughput
            if elapsed > 0.0 && self.prev_messages > 0 {
                let msg_diff = total_messages - self.prev_messages;
                self.current_throughput = msg_diff as f64 / elapsed;

                // Update per-topic throughput (simplified - in reality would track per topic)
                let topic_count = self.topics.len().max(1);
                for topic in &mut self.topics {
                    topic.messages_per_sec = self.current_throughput / topic_count as f64;
                }

                // Add to history
                self.throughput_history
                    .push_back(self.current_throughput.max(0.0) as u64);
                if self.throughput_history.len() > 60 {
                    self.throughput_history.pop_front();
                }
            }

            self.prev_messages = total_messages;
            self.prev_refresh = Instant::now();

            self.stats.topics_count = self.topics.len();
            self.stats.partitions_count = total_partitions as usize;
            self.stats.messages_total = total_messages;
            self.stats.storage_bytes = total_storage;
            self.stats.uptime_secs = self.server_start.elapsed().as_secs();
        }

        // Refresh groups
        if let Ok(coordinator) = GroupCoordinator::new(data_dir, manager.clone()) {
            if let Ok(group_ids) = coordinator.list_groups() {
                self.groups = group_ids
                    .iter()
                    .filter_map(|id| {
                        let group = coordinator.get_group(id).ok()??;

                        // Get unique topics from offsets
                        let topics: Vec<String> = group
                            .offsets
                            .keys()
                            .map(|(t, _)| t.clone())
                            .collect::<std::collections::HashSet<_>>()
                            .into_iter()
                            .collect();

                        // Calculate lag
                        let mut total_lag = 0i64;
                        for ((topic, partition), committed) in &group.offsets {
                            if let Ok(latest) = manager.latest_offset(topic, *partition) {
                                total_lag += (latest - committed.offset).max(0);
                            }
                        }

                        Some(GroupInfo {
                            id: id.clone(),
                            state: format!("{:?}", group.state),
                            members: group.members.len(),
                            topics,
                            total_lag,
                            bookmarked: self.bookmarked_groups.contains(id),
                        })
                    })
                    .collect();

                self.stats.groups_count = self.groups.len();
            }
        }

        // Load partition details if viewing topic detail
        if let Some(ref topic) = self.detail_topic {
            self.partition_details = (0..topic.partitions)
                .filter_map(|p| {
                    let earliest = manager.earliest_offset(&topic.name, p).ok()?;
                    let latest = manager.latest_offset(&topic.name, p).ok()?;
                    Some(PartitionInfo {
                        partition: p,
                        earliest_offset: earliest,
                        latest_offset: latest,
                        messages: latest - earliest,
                        size_bytes: ((latest - earliest) as u64) * 500, // Estimate
                    })
                })
                .collect();
        }

        // Check for alerts
        self.check_alerts();

        self.last_refresh = Instant::now();
    }

    fn open_topic_detail(&mut self) {
        let topics = self.filtered_topics();
        if let Some(topic) = topics.get(self.topic_selected) {
            self.detail_topic = Some((*topic).clone());
            self.view_mode = ViewMode::TopicDetail;
        }
    }

    fn open_group_detail(&mut self) {
        let groups = self.filtered_groups();
        if let Some(group) = groups.get(self.group_selected) {
            self.detail_group = Some((*group).clone());
            self.view_mode = ViewMode::GroupDetail;
        }
    }
}

/// Run the TUI dashboard
pub fn run_dashboard(config: &DashboardConfig) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create state
    let mut state = DashboardState::new();
    state.refresh(&config.data_dir);

    // Add welcome alert
    state.add_alert(
        AlertSeverity::Info,
        "Dashboard started. Press ? for help.".to_string(),
    );

    // Main loop
    let tick_rate = config.refresh_interval;
    let mut last_tick = Instant::now();
    let mut status_message: Option<(String, Instant)> = None;

    loop {
        terminal.draw(|f| ui(f, &state, &status_message))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            match event::read()? {
                Event::Key(key) => {
                    // Clear status message on any key
                    status_message = None;

                    match state.view_mode {
                        ViewMode::Search => match key.code {
                            KeyCode::Esc => {
                                state.view_mode = ViewMode::Normal;
                                state.search_query.clear();
                            }
                            KeyCode::Enter => {
                                state.view_mode = ViewMode::Normal;
                            }
                            KeyCode::Backspace => {
                                state.search_query.pop();
                            }
                            KeyCode::Char(c) => {
                                state.search_query.push(c);
                            }
                            _ => {}
                        },
                        ViewMode::Command => match key.code {
                            KeyCode::Esc => {
                                state.view_mode = ViewMode::Normal;
                                state.command_input.clear();
                            }
                            KeyCode::Enter => {
                                if let Some(msg) = state.execute_command(&config.data_dir) {
                                    status_message = Some((msg, Instant::now()));
                                }
                                state.command_input.clear();
                                state.view_mode = ViewMode::Normal;
                            }
                            KeyCode::Backspace => {
                                state.command_input.pop();
                            }
                            KeyCode::Char(c) => {
                                state.command_input.push(c);
                            }
                            _ => {}
                        },
                        ViewMode::Help
                        | ViewMode::TopicDetail
                        | ViewMode::GroupDetail
                        | ViewMode::LiveMessages => match key.code {
                            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => {
                                state.view_mode = ViewMode::Normal;
                                state.detail_topic = None;
                                state.detail_group = None;
                            }
                            KeyCode::Up | KeyCode::Char('k')
                                if state.view_mode == ViewMode::LiveMessages =>
                            {
                                state.live_message_scroll =
                                    state.live_message_scroll.saturating_sub(1);
                            }
                            KeyCode::Down | KeyCode::Char('j')
                                if state.view_mode == ViewMode::LiveMessages =>
                            {
                                if state.live_message_scroll
                                    < state.live_messages.len().saturating_sub(1)
                                {
                                    state.live_message_scroll += 1;
                                }
                            }
                            _ => {}
                        },
                        ViewMode::Normal => {
                            match key.code {
                                KeyCode::Char('q') => state.should_quit = true,
                                KeyCode::Char('c')
                                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                                {
                                    state.should_quit = true
                                }
                                KeyCode::Tab => state.next_tab(),
                                KeyCode::BackTab => state.prev_tab(),
                                KeyCode::Char('1') => state.tab_index = 0,
                                KeyCode::Char('2') => state.tab_index = 1,
                                KeyCode::Char('3') => state.tab_index = 2,
                                KeyCode::Char('4') => state.tab_index = 3,
                                KeyCode::Char('5') => state.tab_index = 4,
                                KeyCode::Char('?') | KeyCode::Char('h') => {
                                    state.view_mode = ViewMode::Help;
                                }
                                KeyCode::Char('/') => {
                                    state.view_mode = ViewMode::Search;
                                    state.search_query.clear();
                                }
                                KeyCode::Char(':') => {
                                    state.view_mode = ViewMode::Command;
                                    state.command_input.clear();
                                }
                                KeyCode::Char('s') => {
                                    if state.tab_index == 1 {
                                        state.topic_sort = state.topic_sort.next();
                                        state.save_state();
                                    } else if state.tab_index == 2 {
                                        state.group_sort = state.group_sort.next();
                                        state.save_state();
                                    }
                                }
                                KeyCode::Char('S') => {
                                    if state.tab_index == 1 {
                                        state.topic_sort_asc = !state.topic_sort_asc;
                                        state.save_state();
                                    } else if state.tab_index == 2 {
                                        state.group_sort_asc = !state.group_sort_asc;
                                        state.save_state();
                                    }
                                }
                                KeyCode::Char('t') => {
                                    state.theme = state.theme.toggle();
                                    state.save_state();
                                }
                                KeyCode::Char('b') => {
                                    if state.tab_index == 1 {
                                        state.toggle_topic_bookmark();
                                    } else if state.tab_index == 2 {
                                        state.toggle_group_bookmark();
                                    }
                                }
                                KeyCode::Char('e') => {
                                    let msg = if state.tab_index == 1 {
                                        state.export_topics()
                                    } else if state.tab_index == 2 {
                                        state.export_groups()
                                    } else {
                                        "Export: switch to Topics (2) or Groups (3) tab".to_string()
                                    };
                                    status_message = Some((msg, Instant::now()));
                                }
                                KeyCode::Char('l') if state.tab_index == 1 => {
                                    state.view_mode = ViewMode::LiveMessages;
                                }
                                KeyCode::Enter => {
                                    if state.tab_index == 1 {
                                        state.open_topic_detail();
                                    } else if state.tab_index == 2 {
                                        state.open_group_detail();
                                    }
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    if state.tab_index == 1 {
                                        state.topic_selected =
                                            state.topic_selected.saturating_sub(1);
                                        // Scroll up if needed
                                        if state.topic_selected < state.topic_scroll {
                                            state.topic_scroll = state.topic_selected;
                                        }
                                    } else if state.tab_index == 2 {
                                        state.group_selected =
                                            state.group_selected.saturating_sub(1);
                                        if state.group_selected < state.group_scroll {
                                            state.group_scroll = state.group_selected;
                                        }
                                    }
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    if state.tab_index == 1 {
                                        let max = state.filtered_topics().len().saturating_sub(1);
                                        if state.topic_selected < max {
                                            state.topic_selected += 1;
                                        }
                                    } else if state.tab_index == 2 {
                                        let max = state.filtered_groups().len().saturating_sub(1);
                                        if state.group_selected < max {
                                            state.group_selected += 1;
                                        }
                                    }
                                }
                                KeyCode::PageUp => {
                                    if state.tab_index == 1 {
                                        state.topic_selected =
                                            state.topic_selected.saturating_sub(10);
                                        state.topic_scroll = state.topic_scroll.saturating_sub(10);
                                    } else if state.tab_index == 2 {
                                        state.group_selected =
                                            state.group_selected.saturating_sub(10);
                                        state.group_scroll = state.group_scroll.saturating_sub(10);
                                    }
                                }
                                KeyCode::PageDown => {
                                    if state.tab_index == 1 {
                                        let max = state.filtered_topics().len().saturating_sub(1);
                                        state.topic_selected = (state.topic_selected + 10).min(max);
                                    } else if state.tab_index == 2 {
                                        let max = state.filtered_groups().len().saturating_sub(1);
                                        state.group_selected = (state.group_selected + 10).min(max);
                                    }
                                }
                                KeyCode::Home => {
                                    if state.tab_index == 1 {
                                        state.topic_selected = 0;
                                        state.topic_scroll = 0;
                                    } else if state.tab_index == 2 {
                                        state.group_selected = 0;
                                        state.group_scroll = 0;
                                    }
                                }
                                KeyCode::End => {
                                    if state.tab_index == 1 {
                                        state.topic_selected =
                                            state.filtered_topics().len().saturating_sub(1);
                                    } else if state.tab_index == 2 {
                                        state.group_selected =
                                            state.filtered_groups().len().saturating_sub(1);
                                    }
                                }
                                KeyCode::Char('r') => state.refresh(&config.data_dir),
                                KeyCode::Esc => {
                                    state.search_query.clear();
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Event::Mouse(mouse) if state.mouse_enabled => {
                    handle_mouse_event(&mut state, mouse);
                }
                _ => {}
            }
        }

        if last_tick.elapsed() >= tick_rate {
            state.refresh(&config.data_dir);
            last_tick = Instant::now();
        }

        // Clear old status messages
        if let Some((_, time)) = &status_message {
            if time.elapsed() > Duration::from_secs(3) {
                status_message = None;
            }
        }

        if state.should_quit {
            break;
        }
    }

    // Save state before exiting
    state.save_state();

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

fn handle_mouse_event(state: &mut DashboardState, mouse: MouseEvent) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            // Check if clicking on tabs (row 1-2)
            if mouse.row < 3 {
                // Calculate which tab was clicked based on column
                let tab_width = 15; // Approximate
                let tab_index = (mouse.column as usize) / tab_width;
                if tab_index < state.tabs.len() {
                    state.tab_index = tab_index;
                }
            }
        }
        MouseEventKind::ScrollUp => {
            if state.tab_index == 1 {
                state.topic_selected = state.topic_selected.saturating_sub(3);
            } else if state.tab_index == 2 {
                state.group_selected = state.group_selected.saturating_sub(3);
            }
        }
        MouseEventKind::ScrollDown => {
            if state.tab_index == 1 {
                let max = state.filtered_topics().len().saturating_sub(1);
                state.topic_selected = (state.topic_selected + 3).min(max);
            } else if state.tab_index == 2 {
                let max = state.filtered_groups().len().saturating_sub(1);
                state.group_selected = (state.group_selected + 3).min(max);
            }
        }
        _ => {}
    }
}

fn ui(f: &mut Frame, state: &DashboardState, status_message: &Option<(String, Instant)>) {
    let theme = state.theme;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Content
            Constraint::Length(2), // Footer (increased for status)
        ])
        .split(f.area());

    // Header with tabs
    let titles: Vec<Line> = state
        .tabs
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let style = if i == state.tab_index {
                Style::default()
                    .fg(theme.highlight())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.fg())
            };
            Line::from(Span::styled(format!(" {} ", t), style))
        })
        .collect();

    let mut header_title = " Streamline Dashboard ".to_string();
    if !state.search_query.is_empty() {
        header_title = format!(" Streamline Dashboard | Filter: {} ", state.search_query);
    }

    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(header_title)
                .border_style(Style::default().fg(theme.accent())),
        )
        .select(state.tab_index)
        .style(Style::default().fg(theme.accent()))
        .highlight_style(
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )
        .divider(symbols::line::VERTICAL);

    f.render_widget(tabs, chunks[0]);

    // Content based on selected tab
    match state.tab_index {
        0 => render_overview(f, chunks[1], state, theme),
        1 => render_topics(f, chunks[1], state, theme),
        2 => render_groups(f, chunks[1], state, theme),
        3 => render_performance(f, chunks[1], state, theme),
        4 => render_alerts(f, chunks[1], state, theme),
        _ => {}
    }

    // Footer
    let footer_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(chunks[2]);

    // Keyboard shortcuts
    let shortcuts = match state.view_mode {
        ViewMode::Search => vec![
            Span::styled(" ESC", Style::default().fg(theme.highlight())),
            Span::raw(" cancel  "),
            Span::styled("Enter", Style::default().fg(theme.highlight())),
            Span::raw(" apply"),
        ],
        ViewMode::Command => vec![
            Span::styled(" ESC", Style::default().fg(theme.highlight())),
            Span::raw(" cancel  "),
            Span::styled("Enter", Style::default().fg(theme.highlight())),
            Span::raw(" execute"),
        ],
        _ => vec![
            Span::styled(" q", Style::default().fg(theme.highlight())),
            Span::raw(" quit  "),
            Span::styled("Tab", Style::default().fg(theme.highlight())),
            Span::raw(" switch  "),
            Span::styled("/", Style::default().fg(theme.highlight())),
            Span::raw(" search  "),
            Span::styled(":", Style::default().fg(theme.highlight())),
            Span::raw(" command  "),
            Span::styled("s", Style::default().fg(theme.highlight())),
            Span::raw(" sort  "),
            Span::styled("?", Style::default().fg(theme.highlight())),
            Span::raw(" help"),
        ],
    };

    let footer = Paragraph::new(Line::from(shortcuts)).style(Style::default().fg(theme.muted()));
    f.render_widget(footer, footer_chunks[0]);

    // Status message or input
    let status_line = if state.view_mode == ViewMode::Search {
        Line::from(vec![
            Span::styled(" /", Style::default().fg(theme.highlight())),
            Span::raw(&state.search_query),
            Span::styled("█", Style::default().fg(theme.highlight())),
        ])
    } else if state.view_mode == ViewMode::Command {
        Line::from(vec![
            Span::styled(" :", Style::default().fg(theme.highlight())),
            Span::raw(&state.command_input),
            Span::styled("█", Style::default().fg(theme.highlight())),
        ])
    } else if let Some((msg, _)) = status_message {
        Line::from(vec![
            Span::styled(" → ", Style::default().fg(theme.accent())),
            Span::raw(msg),
        ])
    } else {
        Line::from(vec![
            Span::styled(" Last refresh: ", Style::default().fg(theme.muted())),
            Span::raw(format!("{}s ago", state.last_refresh.elapsed().as_secs())),
            Span::raw("  "),
            Span::styled("Uptime: ", Style::default().fg(theme.muted())),
            Span::raw(format_duration(state.stats.uptime_secs)),
        ])
    };

    let status = Paragraph::new(status_line).style(Style::default().fg(theme.muted()));
    f.render_widget(status, footer_chunks[1]);

    // Overlays
    match state.view_mode {
        ViewMode::Help => render_help(f, theme),
        ViewMode::TopicDetail => render_topic_detail(f, state, theme),
        ViewMode::GroupDetail => render_group_detail(f, state, theme),
        ViewMode::LiveMessages => render_live_messages(f, state, theme),
        _ => {}
    }
}

fn render_overview(f: &mut Frame, area: Rect, state: &DashboardState, theme: Theme) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // Stats
            Constraint::Length(5), // Throughput sparkline
            Constraint::Min(0),    // Quick info
        ])
        .split(area);

    let stats_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(chunks[0]);

    // Stat boxes
    render_stat_box(
        f,
        stats_chunks[0],
        "Topics",
        &state.stats.topics_count.to_string(),
        theme.success(),
        theme,
    );
    render_stat_box(
        f,
        stats_chunks[1],
        "Partitions",
        &state.stats.partitions_count.to_string(),
        theme.accent(),
        theme,
    );
    render_stat_box(
        f,
        stats_chunks[2],
        "Groups",
        &state.stats.groups_count.to_string(),
        theme.highlight(),
        theme,
    );
    render_stat_box(
        f,
        stats_chunks[3],
        "Messages",
        &format_number(state.stats.messages_total as u64),
        theme.success(),
        theme,
    );

    // Throughput sparkline
    let sparkline_data: Vec<u64> = state.throughput_history.iter().copied().collect();
    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(
                    " Throughput: {:.0} msg/s ",
                    state.current_throughput
                ))
                .border_style(Style::default().fg(theme.accent())),
        )
        .data(&sparkline_data)
        .style(Style::default().fg(theme.success()));
    f.render_widget(sparkline, chunks[1]);

    // Quick info with gauges
    let info_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[2]);

    // Storage gauge
    let storage_percent = if state.stats.storage_bytes > 0 {
        // Assume 100GB max for demo
        ((state.stats.storage_bytes as f64 / 100_000_000_000.0) * 100.0).min(100.0) as u16
    } else {
        0
    };

    let storage_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Storage Usage ")
                .border_style(Style::default().fg(theme.accent())),
        )
        .gauge_style(Style::default().fg(if storage_percent > 80 {
            theme.error()
        } else {
            theme.success()
        }))
        .percent(storage_percent)
        .label(format!("{} used", format_bytes(state.stats.storage_bytes)));
    f.render_widget(storage_gauge, info_chunks[0]);

    // Server info
    let info_items = vec![
        ListItem::new(Line::from(vec![
            Span::styled("Status: ", Style::default().fg(theme.muted())),
            Span::styled("● Running", Style::default().fg(theme.success())),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Version: ", Style::default().fg(theme.muted())),
            Span::styled(
                env!("CARGO_PKG_VERSION"),
                Style::default().fg(theme.accent()),
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Uptime: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_duration(state.stats.uptime_secs),
                Style::default().fg(theme.fg()),
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Theme: ", Style::default().fg(theme.muted())),
            Span::styled(
                if theme == Theme::Dark {
                    "Dark"
                } else {
                    "Light"
                },
                Style::default().fg(theme.fg()),
            ),
            Span::raw(" (press t to toggle)"),
        ])),
    ];

    let info_list = List::new(info_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Server Info ")
            .border_style(Style::default().fg(theme.accent())),
    );
    f.render_widget(info_list, info_chunks[1]);
}

fn render_stat_box(
    f: &mut Frame,
    area: Rect,
    title: &str,
    value: &str,
    color: Color,
    theme: Theme,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", title))
        .border_style(Style::default().fg(theme.accent()));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let text = Paragraph::new(Line::from(vec![Span::styled(
        value,
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    )]))
    .alignment(ratatui::layout::Alignment::Center);

    // Center vertically
    let centered_area = Rect {
        x: inner.x,
        y: inner.y + inner.height / 2,
        width: inner.width,
        height: 1,
    };
    f.render_widget(text, centered_area);
}

fn render_topics(f: &mut Frame, area: Rect, state: &DashboardState, theme: Theme) {
    let topics = state.filtered_topics();

    // Sort indicator
    let sort_indicator = |col: TopicSortColumn| -> &str {
        if state.topic_sort == col {
            if state.topic_sort_asc {
                " ▲"
            } else {
                " ▼"
            }
        } else {
            ""
        }
    };

    let header_cells = [
        format!("★ Topic{}", sort_indicator(TopicSortColumn::Name)),
        format!("Partitions{}", sort_indicator(TopicSortColumn::Partitions)),
        format!("Messages{}", sort_indicator(TopicSortColumn::Messages)),
        format!("Size{}", sort_indicator(TopicSortColumn::Size)),
        format!("msg/s{}", sort_indicator(TopicSortColumn::Throughput)),
    ];

    let header = Row::new(header_cells.iter().map(|h| {
        Cell::from(h.as_str()).style(
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )
    }))
    .height(1)
    .bottom_margin(1);

    let rows: Vec<Row> = topics
        .iter()
        .enumerate()
        .map(|(i, topic)| {
            let style = if i == state.topic_selected {
                Style::default()
                    .bg(theme.accent())
                    .fg(theme.bg())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.fg())
            };

            let bookmark = if topic.bookmarked { "★ " } else { "  " };

            Row::new(vec![
                Cell::from(format!("{}{}", bookmark, topic.name)),
                Cell::from(topic.partitions.to_string()),
                Cell::from(format_number(topic.total_messages as u64)),
                Cell::from(format_bytes(topic.size_bytes)),
                Cell::from(format!("{:.1}", topic.messages_per_sec)),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(35),
            Constraint::Percentage(15),
            Constraint::Percentage(20),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Topics ({}) │ s:sort S:reverse b:bookmark Enter:detail l:live ",
                topics.len()
            ))
            .border_style(Style::default().fg(theme.accent())),
    );

    f.render_widget(table, area);

    // Scrollbar
    if topics.len() > (area.height as usize - 4) {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        let mut scrollbar_state = ScrollbarState::new(topics.len()).position(state.topic_selected);
        f.render_stateful_widget(scrollbar, area, &mut scrollbar_state);
    }
}

fn render_groups(f: &mut Frame, area: Rect, state: &DashboardState, theme: Theme) {
    let groups = state.filtered_groups();

    let sort_indicator = |col: GroupSortColumn| -> &str {
        if state.group_sort == col {
            if state.group_sort_asc {
                " ▲"
            } else {
                " ▼"
            }
        } else {
            ""
        }
    };

    let header_cells = [
        format!("★ Group ID{}", sort_indicator(GroupSortColumn::Id)),
        format!("State{}", sort_indicator(GroupSortColumn::State)),
        format!("Members{}", sort_indicator(GroupSortColumn::Members)),
        "Topics".to_string(),
        format!("Lag{}", sort_indicator(GroupSortColumn::Lag)),
    ];

    let header = Row::new(header_cells.iter().map(|h| {
        Cell::from(h.as_str()).style(
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )
    }))
    .height(1)
    .bottom_margin(1);

    let rows: Vec<Row> = groups
        .iter()
        .enumerate()
        .map(|(i, group)| {
            let style = if i == state.group_selected {
                Style::default()
                    .bg(theme.accent())
                    .fg(theme.bg())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.fg())
            };

            let lag_style = if group.total_lag > 100_000 {
                Style::default().fg(theme.error())
            } else if group.total_lag > 10_000 {
                Style::default().fg(theme.warning())
            } else {
                Style::default().fg(theme.success())
            };

            let state_style = match group.state.as_str() {
                "Stable" => Style::default().fg(theme.success()),
                "PreparingRebalance" | "CompletingRebalance" => {
                    Style::default().fg(theme.warning())
                }
                "Dead" | "Empty" => Style::default().fg(theme.error()),
                _ => Style::default().fg(theme.fg()),
            };

            let bookmark = if group.bookmarked { "★ " } else { "  " };

            Row::new(vec![
                Cell::from(format!("{}{}", bookmark, group.id)),
                Cell::from(group.state.clone()).style(state_style),
                Cell::from(group.members.to_string()),
                Cell::from(group.topics.len().to_string()),
                Cell::from(format_number(group.total_lag as u64)).style(lag_style),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(35),
            Constraint::Percentage(20),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Consumer Groups ({}) │ s:sort S:reverse b:bookmark Enter:detail ",
                groups.len()
            ))
            .border_style(Style::default().fg(theme.accent())),
    );

    f.render_widget(table, area);

    // Scrollbar
    if groups.len() > (area.height as usize - 4) {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        let mut scrollbar_state = ScrollbarState::new(groups.len()).position(state.group_selected);
        f.render_stateful_widget(scrollbar, area, &mut scrollbar_state);
    }
}

fn render_performance(f: &mut Frame, area: Rect, state: &DashboardState, theme: Theme) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7), // Throughput sparkline
            Constraint::Length(5), // Gauges
            Constraint::Min(0),    // Metrics
        ])
        .split(area);

    // Large throughput sparkline
    let sparkline_data: Vec<u64> = state.throughput_history.iter().copied().collect();
    let max_throughput = sparkline_data.iter().max().copied().unwrap_or(1);

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(
                    " Throughput (current: {:.0} msg/s, max: {} msg/s) ",
                    state.current_throughput,
                    format_number(max_throughput)
                ))
                .border_style(Style::default().fg(theme.accent())),
        )
        .data(&sparkline_data)
        .max(max_throughput.max(100))
        .style(Style::default().fg(theme.success()));
    f.render_widget(sparkline, chunks[0]);

    // Gauges row
    let gauge_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    // Topics gauge
    let topics_percent = ((state.stats.topics_count as f64 / 100.0) * 100.0).min(100.0) as u16;
    let topics_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Topics ")
                .border_style(Style::default().fg(theme.accent())),
        )
        .gauge_style(Style::default().fg(theme.success()))
        .percent(topics_percent)
        .label(format!("{} topics", state.stats.topics_count));
    f.render_widget(topics_gauge, gauge_chunks[0]);

    // Groups gauge
    let groups_percent = ((state.stats.groups_count as f64 / 50.0) * 100.0).min(100.0) as u16;
    let groups_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Consumer Groups ")
                .border_style(Style::default().fg(theme.accent())),
        )
        .gauge_style(Style::default().fg(theme.highlight()))
        .percent(groups_percent)
        .label(format!("{} groups", state.stats.groups_count));
    f.render_widget(groups_gauge, gauge_chunks[1]);

    // Performance metrics
    let metrics = vec![
        ListItem::new(Line::from(vec![
            Span::styled("Current Throughput: ", Style::default().fg(theme.muted())),
            Span::styled(
                format!("{:.2} msg/s", state.current_throughput),
                Style::default().fg(theme.success()),
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Total Messages: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_number(state.stats.messages_total as u64),
                Style::default().fg(theme.accent()),
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Storage Used: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_bytes(state.stats.storage_bytes),
                Style::default().fg(theme.fg()),
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled("Avg Message Size: ", Style::default().fg(theme.muted())),
            Span::styled(
                if state.stats.messages_total > 0 {
                    format_bytes(state.stats.storage_bytes / state.stats.messages_total as u64)
                } else {
                    "-".to_string()
                },
                Style::default().fg(theme.fg()),
            ),
        ])),
        ListItem::new(Line::from("")),
        ListItem::new(Line::from(vec![
            Span::styled(
                "💡 Tip: ",
                Style::default()
                    .fg(theme.highlight())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("Run "),
            Span::styled(
                "streamline-cli benchmark",
                Style::default().fg(theme.accent()),
            ),
            Span::raw(" for detailed performance testing"),
        ])),
    ];

    let metrics_list = List::new(metrics).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Performance Metrics ")
            .border_style(Style::default().fg(theme.accent())),
    );
    f.render_widget(metrics_list, chunks[2]);
}

fn render_alerts(f: &mut Frame, area: Rect, state: &DashboardState, theme: Theme) {
    let items: Vec<ListItem> = state
        .alerts
        .iter()
        .rev() // Most recent first
        .map(|alert| {
            let (icon, color) = match alert.severity {
                AlertSeverity::Info => ("ℹ", theme.accent()),
                AlertSeverity::Warning => ("⚠", theme.warning()),
                AlertSeverity::Error => ("✗", theme.error()),
            };

            let age = alert.timestamp.elapsed().as_secs();
            let age_str = if age < 60 {
                format!("{}s ago", age)
            } else if age < 3600 {
                format!("{}m ago", age / 60)
            } else {
                format!("{}h ago", age / 3600)
            };

            ListItem::new(Line::from(vec![
                Span::styled(format!(" {} ", icon), Style::default().fg(color)),
                Span::raw(&alert.message),
                Span::styled(
                    format!("  ({})", age_str),
                    Style::default().fg(theme.muted()),
                ),
            ]))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Alerts ({}) │ :clear to dismiss all ",
                state.alerts.len()
            ))
            .border_style(Style::default().fg(theme.accent())),
    );

    f.render_widget(list, area);
}

fn render_help(f: &mut Frame, theme: Theme) {
    let area = centered_rect(70, 80, f.area());

    let help_text = vec![
        Line::from(vec![Span::styled(
            "Keyboard Shortcuts",
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Navigation",
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  q, Ctrl+C     ", Style::default().fg(theme.highlight())),
            Span::raw("Quit dashboard"),
        ]),
        Line::from(vec![
            Span::styled("  Tab/Shift+Tab ", Style::default().fg(theme.highlight())),
            Span::raw("Switch tabs"),
        ]),
        Line::from(vec![
            Span::styled("  1-5           ", Style::default().fg(theme.highlight())),
            Span::raw("Jump to tab"),
        ]),
        Line::from(vec![
            Span::styled("  j/k, ↑/↓      ", Style::default().fg(theme.highlight())),
            Span::raw("Navigate list"),
        ]),
        Line::from(vec![
            Span::styled("  PgUp/PgDn     ", Style::default().fg(theme.highlight())),
            Span::raw("Scroll page"),
        ]),
        Line::from(vec![
            Span::styled("  Home/End      ", Style::default().fg(theme.highlight())),
            Span::raw("Jump to start/end"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Actions",
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  Enter         ", Style::default().fg(theme.highlight())),
            Span::raw("Open detail view"),
        ]),
        Line::from(vec![
            Span::styled("  /             ", Style::default().fg(theme.highlight())),
            Span::raw("Search/filter"),
        ]),
        Line::from(vec![
            Span::styled("  :             ", Style::default().fg(theme.highlight())),
            Span::raw("Command mode"),
        ]),
        Line::from(vec![
            Span::styled("  s             ", Style::default().fg(theme.highlight())),
            Span::raw("Cycle sort column"),
        ]),
        Line::from(vec![
            Span::styled("  S             ", Style::default().fg(theme.highlight())),
            Span::raw("Toggle sort direction"),
        ]),
        Line::from(vec![
            Span::styled("  b             ", Style::default().fg(theme.highlight())),
            Span::raw("Toggle bookmark"),
        ]),
        Line::from(vec![
            Span::styled("  e             ", Style::default().fg(theme.highlight())),
            Span::raw("Export to JSON"),
        ]),
        Line::from(vec![
            Span::styled("  l             ", Style::default().fg(theme.highlight())),
            Span::raw("Live message view (Topics tab)"),
        ]),
        Line::from(vec![
            Span::styled("  t             ", Style::default().fg(theme.highlight())),
            Span::raw("Toggle theme"),
        ]),
        Line::from(vec![
            Span::styled("  r             ", Style::default().fg(theme.highlight())),
            Span::raw("Refresh data"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Commands (:)",
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("  :q, :quit     ", Style::default().fg(theme.highlight())),
            Span::raw("Quit"),
        ]),
        Line::from(vec![
            Span::styled("  :export       ", Style::default().fg(theme.highlight())),
            Span::raw("Export topics|groups"),
        ]),
        Line::from(vec![
            Span::styled("  :theme        ", Style::default().fg(theme.highlight())),
            Span::raw("Toggle theme"),
        ]),
        Line::from(vec![
            Span::styled("  :clear        ", Style::default().fg(theme.highlight())),
            Span::raw("Clear alerts"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Press ESC or Enter to close",
            Style::default().fg(theme.muted()),
        )]),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Help ")
        .border_style(Style::default().fg(theme.accent()))
        .style(Style::default().bg(theme.bg()));

    let paragraph = Paragraph::new(help_text)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(Clear, area);
    f.render_widget(paragraph, area);
}

fn render_topic_detail(f: &mut Frame, state: &DashboardState, theme: Theme) {
    let area = centered_rect(80, 70, f.area());

    let topic = match &state.detail_topic {
        Some(t) => t,
        None => return,
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Topic: ", Style::default().fg(theme.muted())),
            Span::styled(
                &topic.name,
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("Partitions: ", Style::default().fg(theme.muted())),
            Span::styled(
                topic.partitions.to_string(),
                Style::default().fg(theme.fg()),
            ),
        ]),
        Line::from(vec![
            Span::styled("Total Messages: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_number(topic.total_messages as u64),
                Style::default().fg(theme.success()),
            ),
        ]),
        Line::from(vec![
            Span::styled("Size: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_bytes(topic.size_bytes),
                Style::default().fg(theme.fg()),
            ),
        ]),
        Line::from(vec![
            Span::styled("Throughput: ", Style::default().fg(theme.muted())),
            Span::styled(
                format!("{:.1} msg/s", topic.messages_per_sec),
                Style::default().fg(theme.fg()),
            ),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Partitions:",
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(""),
    ];

    // Partition table header
    lines.push(Line::from(vec![
        Span::styled("  #   ", Style::default().fg(theme.highlight())),
        Span::styled("Earliest   ", Style::default().fg(theme.highlight())),
        Span::styled("Latest     ", Style::default().fg(theme.highlight())),
        Span::styled("Messages   ", Style::default().fg(theme.highlight())),
        Span::styled("Size", Style::default().fg(theme.highlight())),
    ]));

    for p in &state.partition_details {
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {:<3} ", p.partition),
                Style::default().fg(theme.muted()),
            ),
            Span::raw(format!("{:<10} ", p.earliest_offset)),
            Span::raw(format!("{:<10} ", p.latest_offset)),
            Span::styled(
                format!("{:<10} ", format_number(p.messages as u64)),
                Style::default().fg(theme.success()),
            ),
            Span::raw(format_bytes(p.size_bytes)),
        ]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "Press ESC to close",
        Style::default().fg(theme.muted()),
    )]));

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Topic Details ")
        .border_style(Style::default().fg(theme.accent()))
        .style(Style::default().bg(theme.bg()));

    let paragraph = Paragraph::new(lines).block(block);

    f.render_widget(Clear, area);
    f.render_widget(paragraph, area);
}

fn render_group_detail(f: &mut Frame, state: &DashboardState, theme: Theme) {
    let area = centered_rect(70, 60, f.area());

    let group = match &state.detail_group {
        Some(g) => g,
        None => return,
    };

    let state_color = match group.state.as_str() {
        "Stable" => theme.success(),
        "PreparingRebalance" | "CompletingRebalance" => theme.warning(),
        "Dead" | "Empty" => theme.error(),
        _ => theme.fg(),
    };

    let lag_color = if group.total_lag > 100_000 {
        theme.error()
    } else if group.total_lag > 10_000 {
        theme.warning()
    } else {
        theme.success()
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("Group ID: ", Style::default().fg(theme.muted())),
            Span::styled(
                &group.id,
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("State: ", Style::default().fg(theme.muted())),
            Span::styled(&group.state, Style::default().fg(state_color)),
        ]),
        Line::from(vec![
            Span::styled("Members: ", Style::default().fg(theme.muted())),
            Span::styled(group.members.to_string(), Style::default().fg(theme.fg())),
        ]),
        Line::from(vec![
            Span::styled("Total Lag: ", Style::default().fg(theme.muted())),
            Span::styled(
                format_number(group.total_lag as u64),
                Style::default().fg(lag_color),
            ),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Subscribed Topics:",
            Style::default()
                .fg(theme.highlight())
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(
                if group.topics.is_empty() {
                    "(none)".to_string()
                } else {
                    group.topics.join(", ")
                },
                Style::default().fg(theme.fg()),
            ),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled(
            "Press ESC to close",
            Style::default().fg(theme.muted()),
        )]),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Consumer Group Details ")
        .border_style(Style::default().fg(theme.accent()))
        .style(Style::default().bg(theme.bg()));

    let paragraph = Paragraph::new(lines).block(block);

    f.render_widget(Clear, area);
    f.render_widget(paragraph, area);
}

fn render_live_messages(f: &mut Frame, state: &DashboardState, theme: Theme) {
    let area = centered_rect(90, 80, f.area());

    let lines: Vec<Line> = state
        .live_messages
        .iter()
        .map(|msg| {
            let age = msg.timestamp.elapsed().as_secs();
            Line::from(vec![
                Span::styled(
                    format!("[{}:{}] ", msg.topic, msg.partition),
                    Style::default().fg(theme.accent()),
                ),
                Span::styled(
                    format!("@{} ", msg.offset),
                    Style::default().fg(theme.muted()),
                ),
                Span::raw(&msg.value_preview),
                Span::styled(format!("  ({}s)", age), Style::default().fg(theme.muted())),
            ])
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Live Messages (placeholder - needs broker connection) ")
        .border_style(Style::default().fg(theme.accent()))
        .style(Style::default().bg(theme.bg()));

    let content = if lines.is_empty() {
        let placeholder = vec![
            Line::from(""),
            Line::from(vec![Span::styled(
                "  Live message streaming requires an active broker connection.",
                Style::default().fg(theme.muted()),
            )]),
            Line::from(""),
            Line::from(vec![Span::styled(
                "  To see live messages, use the CLI:",
                Style::default().fg(theme.muted()),
            )]),
            Line::from(vec![
                Span::raw("    "),
                Span::styled(
                    "streamline-cli consume <topic> -f",
                    Style::default().fg(theme.accent()),
                ),
            ]),
            Line::from(""),
            Line::from(vec![Span::styled(
                "  Press ESC to close",
                Style::default().fg(theme.muted()),
            )]),
        ];
        Paragraph::new(placeholder).block(block)
    } else {
        Paragraph::new(lines).block(block)
    };

    f.render_widget(Clear, area);
    f.render_widget(content, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "-".to_string();
    }
    if bytes >= 1_099_511_627_776 {
        format!("{:.1}TB", bytes as f64 / 1_099_511_627_776.0)
    } else if bytes >= 1_073_741_824 {
        format!("{:.1}GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1}MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}
