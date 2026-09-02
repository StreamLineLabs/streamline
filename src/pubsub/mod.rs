//! Pub/Sub mode for real-time message broadcasting
//!
//! This module provides a Redis-like pub/sub system for real-time message delivery.
//! Unlike the persistent topic-based storage, pub/sub messages are only delivered
//! to currently subscribed clients and are not persisted.
//!
//! # Features
//!
//! - **Channel-based messaging**: Subscribers receive messages from channels they subscribe to
//! - **Pattern subscriptions**: Subscribe to channels matching a glob pattern
//! - **No persistence**: Messages are fire-and-forget, only delivered to active subscribers
//! - **Low latency**: Direct delivery without disk I/O
//! - **Message history**: Optional bounded per-channel message replay
//! - **Persistent subscriptions**: Durable subscriber state with replay-from-offset
//! - **Channel metadata & TTL**: Per-channel configuration, idle expiry
//! - **Enhanced pattern matching**: Glob patterns with `*` and `?` wildcards
//!
//! # Example
//!
//! ```ignore
//! use streamline::pubsub::{PubSubManager, Message};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let pubsub = PubSubManager::new();
//!
//! // Subscribe to a channel
//! let mut receiver = pubsub.subscribe("events").await;
//!
//! // In another task, publish a message
//! pubsub.publish("events", "Hello, World!").await;
//!
//! // Receive the message
//! if let Ok(msg) = receiver.recv().await {
//!     println!("Received: {:?}", msg);
//! }
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

/// Default channel buffer size for broadcast channels
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Message delivered through pub/sub
#[derive(Debug, Clone)]
pub struct Message {
    /// Unique message identifier (assigned by PubSubManager on publish)
    pub id: u64,
    /// Channel the message was published to
    pub channel: String,
    /// Message payload
    pub payload: Bytes,
    /// Timestamp when the message was published (Unix millis)
    pub timestamp: u64,
}

impl Message {
    /// Create a new message with id 0 (assigned on publish)
    pub fn new(channel: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            id: 0,
            channel: channel.into(),
            payload: payload.into(),
            timestamp: now_millis(),
        }
    }
}

/// Statistics for a pub/sub channel
#[derive(Debug, Clone)]
pub struct ChannelStats {
    /// Channel name
    pub name: String,
    /// Number of active subscribers
    pub subscribers: usize,
    /// Total messages published to this channel
    pub messages_published: u64,
}

/// Configuration for an individual channel
#[derive(Debug, Clone, Default)]
pub struct ChannelConfig {
    /// Maximum number of subscribers (0 = unlimited)
    pub max_subscribers: usize,
    /// Message time-to-live in milliseconds (0 = no TTL)
    pub message_ttl_ms: u64,
    /// Number of recent messages to retain in history (0 = disabled)
    pub history_size: usize,
    /// Auto-expire channel after this many ms of inactivity (0 = no expiry)
    pub idle_timeout_ms: u64,
}

/// Channel state including broadcast sender and stats
struct ChannelState {
    /// Broadcast sender for the channel
    sender: broadcast::Sender<Message>,
    /// Number of messages published
    messages_published: AtomicU64,
    /// Bounded message history (ring buffer)
    history: Mutex<VecDeque<Message>>,
    /// Maximum number of messages to retain in history
    history_capacity: usize,
    /// Per-channel configuration (if created with explicit config)
    channel_config: Option<ChannelConfig>,
    /// Timestamp of the last publish or subscribe activity (Unix millis)
    last_activity: AtomicU64,
}

impl ChannelState {
    fn with_options(
        capacity: usize,
        history_capacity: usize,
        channel_config: Option<ChannelConfig>,
    ) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        let now = now_millis();
        Self {
            sender,
            messages_published: AtomicU64::new(0),
            history: Mutex::new(VecDeque::with_capacity(history_capacity)),
            history_capacity,
            channel_config,
            last_activity: AtomicU64::new(now),
        }
    }

    fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    fn touch(&self) {
        self.last_activity.store(now_millis(), Ordering::Relaxed);
    }

    fn push_history(&self, message: &Message) {
        if self.history_capacity == 0 {
            return;
        }
        if let Ok(mut history) = self.history.lock() {
            if history.len() >= self.history_capacity {
                history.pop_front();
            }
            history.push_back(message.clone());
        }
    }

    fn get_history(&self, count: usize) -> Vec<Message> {
        if let Ok(history) = self.history.lock() {
            let ttl = self
                .channel_config
                .as_ref()
                .map(|c| c.message_ttl_ms)
                .unwrap_or(0);
            let now = now_millis();
            let filtered: Vec<Message> = if ttl > 0 {
                history
                    .iter()
                    .filter(|m| now.saturating_sub(m.timestamp) <= ttl)
                    .cloned()
                    .collect()
            } else {
                history.iter().cloned().collect()
            };
            let len = filtered.len();
            if count >= len {
                filtered
            } else {
                filtered[len - count..].to_vec()
            }
        } else {
            Vec::new()
        }
    }
}

/// Configuration for the pub/sub manager
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    /// Maximum number of messages buffered per channel
    pub channel_capacity: usize,
    /// Whether to auto-create channels on first publish
    pub auto_create_channels: bool,
    /// Maximum number of channels allowed (0 = unlimited)
    pub max_channels: usize,
    /// Default number of recent messages to retain per channel (0 = disabled)
    pub history_size: usize,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            auto_create_channels: true,
            max_channels: 0, // Unlimited
            history_size: 0,
        }
    }
}

/// Internal state for a persistent subscription
struct PersistentSubscriptionState {
    subscriber_id: String,
    /// channel_name -> last acknowledged message ID
    channel_offsets: HashMap<String, u64>,
    created_at: u64,
}

/// Manages pub/sub channels and subscriptions
pub struct PubSubManager {
    /// Channels by name
    channels: Arc<RwLock<HashMap<String, Arc<ChannelState>>>>,
    /// Configuration
    config: PubSubConfig,
    /// Total messages published across all channels
    total_messages: AtomicU64,
    /// Total subscriptions created
    total_subscriptions: AtomicU64,
    /// Pattern subscriptions: glob pattern -> broadcast sender
    pattern_subs: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    /// Persistent subscription state: subscriber_id -> state
    persistent_subs: Arc<RwLock<HashMap<String, PersistentSubscriptionState>>>,
    /// Global monotonic message ID counter
    message_counter: AtomicU64,
}

impl PubSubManager {
    /// Create a new pub/sub manager with default configuration
    pub fn new() -> Self {
        Self::with_config(PubSubConfig::default())
    }

    /// Create a new pub/sub manager with custom configuration
    pub fn with_config(config: PubSubConfig) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            config,
            total_messages: AtomicU64::new(0),
            total_subscriptions: AtomicU64::new(0),
            pattern_subs: Arc::new(RwLock::new(HashMap::new())),
            persistent_subs: Arc::new(RwLock::new(HashMap::new())),
            message_counter: AtomicU64::new(1),
        }
    }

    /// Subscribe to a channel
    ///
    /// Returns a receiver that will receive all messages published to the channel.
    /// If the channel doesn't exist and auto_create_channels is enabled, it will be created.
    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let channels = self.channels.read().await;

        if let Some(state) = channels.get(channel) {
            self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
            state.touch();
            debug!(channel, "New subscriber added to existing channel");
            return state.sender.subscribe();
        }
        drop(channels);

        // Channel doesn't exist - create it if auto-create is enabled
        if self.config.auto_create_channels {
            let mut channels = self.channels.write().await;

            // Check max channels limit
            if self.config.max_channels > 0 && channels.len() >= self.config.max_channels {
                // Return a receiver that will immediately fail
                let (tx, rx) = broadcast::channel(1);
                drop(tx);
                return rx;
            }

            // Double-check after acquiring write lock
            let history_size = self.config.history_size;
            let state = channels
                .entry(channel.to_string())
                .or_insert_with(|| {
                    info!(channel, "Created new pub/sub channel");
                    Arc::new(ChannelState::with_options(
                        self.config.channel_capacity,
                        history_size,
                        None,
                    ))
                })
                .clone();

            self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
            state.touch();
            debug!(channel, "New subscriber added to channel");
            state.sender.subscribe()
        } else {
            // Return a receiver that will immediately fail
            let (tx, rx) = broadcast::channel(1);
            drop(tx);
            rx
        }
    }

    /// Subscribe to multiple channels
    ///
    /// Returns a vector of (channel_name, receiver) tuples.
    pub async fn subscribe_multiple(
        &self,
        channels: &[&str],
    ) -> Vec<(String, broadcast::Receiver<Message>)> {
        let mut result = Vec::with_capacity(channels.len());
        for channel in channels {
            let receiver = self.subscribe(channel).await;
            result.push((channel.to_string(), receiver));
        }
        result
    }

    /// Publish a message to a channel
    ///
    /// Returns the number of subscribers that received the message.
    /// Returns 0 if the channel doesn't exist and auto_create_channels is disabled.
    pub async fn publish(&self, channel: &str, payload: impl Into<Bytes>) -> usize {
        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);
        let mut message = Message::new(channel, payload);
        message.id = message_id;

        let channels = self.channels.read().await;

        if let Some(state) = channels.get(channel) {
            state.push_history(&message);
            state.touch();
            let count = state.sender.send(message.clone()).unwrap_or(0);
            state.messages_published.fetch_add(1, Ordering::Relaxed);
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            drop(channels);
            let pattern_count = self.fan_out_to_patterns(channel, &message).await;
            debug!(
                channel,
                subscribers = count,
                pattern_subscribers = pattern_count,
                "Published message to channel"
            );
            return count + pattern_count;
        }
        drop(channels);

        // Channel doesn't exist
        if self.config.auto_create_channels {
            let mut channels = self.channels.write().await;

            // Check max channels limit
            if self.config.max_channels > 0 && channels.len() >= self.config.max_channels {
                return 0;
            }

            let history_size = self.config.history_size;
            let state = channels
                .entry(channel.to_string())
                .or_insert_with(|| {
                    info!(channel, "Created new pub/sub channel on publish");
                    Arc::new(ChannelState::with_options(
                        self.config.channel_capacity,
                        history_size,
                        None,
                    ))
                })
                .clone();

            state.push_history(&message);
            state.touch();
            let count = state.sender.send(message.clone()).unwrap_or(0);
            state.messages_published.fetch_add(1, Ordering::Relaxed);
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            drop(channels);
            let pattern_count = self.fan_out_to_patterns(channel, &message).await;
            debug!(
                channel,
                subscribers = count,
                pattern_subscribers = pattern_count,
                "Published message to channel"
            );
            count + pattern_count
        } else {
            0
        }
    }

    /// Publish a message to multiple channels
    ///
    /// Returns a vector of (channel_name, subscriber_count) tuples.
    pub async fn publish_multiple(
        &self,
        channels: &[&str],
        payload: impl Into<Bytes> + Clone,
    ) -> Vec<(String, usize)> {
        let mut result = Vec::with_capacity(channels.len());
        for channel in channels {
            let count = self.publish(channel, payload.clone()).await;
            result.push((channel.to_string(), count));
        }
        result
    }

    /// Get the number of subscribers for a channel
    pub async fn subscriber_count(&self, channel: &str) -> usize {
        let channels = self.channels.read().await;
        channels
            .get(channel)
            .map(|s| s.subscriber_count())
            .unwrap_or(0)
    }

    /// List all active channels
    pub async fn list_channels(&self) -> Vec<String> {
        let channels = self.channels.read().await;
        channels.keys().cloned().collect()
    }

    /// Get statistics for a specific channel
    pub async fn channel_stats(&self, channel: &str) -> Option<ChannelStats> {
        let channels = self.channels.read().await;
        channels.get(channel).map(|state| ChannelStats {
            name: channel.to_string(),
            subscribers: state.subscriber_count(),
            messages_published: state.messages_published.load(Ordering::Relaxed),
        })
    }

    /// Get statistics for all channels
    pub async fn all_stats(&self) -> PubSubStats {
        let channels = self.channels.read().await;
        let channel_stats: Vec<ChannelStats> = channels
            .iter()
            .map(|(name, state)| ChannelStats {
                name: name.clone(),
                subscribers: state.subscriber_count(),
                messages_published: state.messages_published.load(Ordering::Relaxed),
            })
            .collect();

        let total_subscribers: usize = channel_stats.iter().map(|s| s.subscribers).sum();

        PubSubStats {
            channels: channel_stats,
            total_channels: channels.len(),
            total_subscribers,
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_subscriptions: self.total_subscriptions.load(Ordering::Relaxed),
        }
    }

    /// Remove a channel (all subscribers will be dropped)
    pub async fn remove_channel(&self, channel: &str) -> bool {
        let mut channels = self.channels.write().await;
        let removed = channels.remove(channel).is_some();
        if removed {
            info!(channel, "Removed pub/sub channel");
        }
        removed
    }

    /// Remove channels with no subscribers
    pub async fn cleanup_empty_channels(&self) -> usize {
        let mut channels = self.channels.write().await;
        let before = channels.len();
        channels.retain(|name, state| {
            let has_subscribers = state.subscriber_count() > 0;
            if !has_subscribers {
                debug!(channel = %name, "Cleaned up empty channel");
            }
            has_subscribers
        });
        let removed = before - channels.len();
        if removed > 0 {
            info!(removed, "Cleaned up empty pub/sub channels");
        }
        removed
    }

    // --- Message History ---

    /// Retrieve the last `count` messages from a channel's history.
    /// Returns an empty Vec if the channel doesn't exist or history is disabled.
    pub async fn get_history(&self, channel: &str, count: usize) -> Vec<Message> {
        let channels = self.channels.read().await;
        channels
            .get(channel)
            .map(|state| state.get_history(count))
            .unwrap_or_default()
    }

    /// Subscribe to a channel and optionally receive message history.
    /// If `replay_count > 0` and the channel has history, returns past messages
    /// along with the live receiver.
    pub async fn subscribe_with_history(
        &self,
        channel: &str,
        replay_count: usize,
    ) -> (Vec<Message>, broadcast::Receiver<Message>) {
        let receiver = self.subscribe(channel).await;
        let history = if replay_count > 0 {
            self.get_history(channel, replay_count).await
        } else {
            Vec::new()
        };
        (history, receiver)
    }

    // --- Persistent Subscriptions ---

    /// Subscribe with a durable identity. If the subscriber previously existed,
    /// replays messages from history that have an ID greater than the last
    /// acknowledged message.
    /// Returns (replayed_messages, live_receiver).
    pub async fn subscribe_persistent(
        &self,
        subscriber_id: &str,
        channel: &str,
    ) -> (Vec<Message>, broadcast::Receiver<Message>) {
        let receiver = self.subscribe(channel).await;

        // Use a single write lock for the read-update cycle to avoid race conditions
        let last_seen = {
            let mut subs = self.persistent_subs.write().await;
            let state = subs.entry(subscriber_id.to_string()).or_insert_with(|| {
                PersistentSubscriptionState {
                    subscriber_id: subscriber_id.to_string(),
                    channel_offsets: HashMap::new(),
                    created_at: now_millis(),
                }
            });
            *state
                .channel_offsets
                .entry(channel.to_string())
                .or_insert(0)
        };

        // Replay from history
        let replay = {
            let channels = self.channels.read().await;
            if let Some(ch_state) = channels.get(channel) {
                if let Ok(history) = ch_state.history.lock() {
                    history
                        .iter()
                        .filter(|m| m.id > last_seen)
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };

        debug!(
            subscriber_id,
            channel,
            replayed = replay.len(),
            "Persistent subscriber connected"
        );

        (replay, receiver)
    }

    /// Acknowledge that a persistent subscriber has processed messages up to
    /// (and including) the given message ID on the specified channel.
    /// Returns `true` if the acknowledgment was recorded.
    pub async fn acknowledge(&self, subscriber_id: &str, channel: &str, message_id: u64) -> bool {
        let mut subs = self.persistent_subs.write().await;
        if let Some(state) = subs.get_mut(subscriber_id) {
            let offset = state
                .channel_offsets
                .entry(channel.to_string())
                .or_insert(0);
            if message_id > *offset {
                *offset = message_id;
                debug!(subscriber_id, channel, message_id, "Acknowledged message");
                return true;
            }
        }
        false
    }

    /// Get the current state of a persistent subscription.
    pub async fn get_persistent_subscription(
        &self,
        subscriber_id: &str,
    ) -> Option<PersistentSubscription> {
        let subs = self.persistent_subs.read().await;
        subs.get(subscriber_id).map(|state| PersistentSubscription {
            subscriber_id: state.subscriber_id.clone(),
            channels: state.channel_offsets.keys().cloned().collect(),
            last_seen: state.channel_offsets.clone(),
            created_at: state.created_at,
        })
    }

    // --- TTL Cleanup ---

    /// Proactively remove expired messages from all channels' history buffers.
    /// Returns the total number of messages removed across all channels.
    pub async fn cleanup_expired_messages(&self) -> usize {
        let channels = self.channels.read().await;
        let now = now_millis();
        let mut total_removed = 0;

        for (_name, state) in channels.iter() {
            let ttl = state
                .channel_config
                .as_ref()
                .map(|c| c.message_ttl_ms)
                .unwrap_or(0);
            if ttl == 0 {
                continue;
            }
            if let Ok(mut history) = state.history.lock() {
                let before = history.len();
                history.retain(|m| now.saturating_sub(m.timestamp) <= ttl);
                total_removed += before - history.len();
            }
        }

        if total_removed > 0 {
            debug!(
                total_removed,
                "Cleaned up expired messages from history buffers"
            );
        }
        total_removed
    }

    /// Spawn a background task that periodically runs `cleanup_expired_messages`.
    /// The task runs every `interval` duration. Returns a `JoinHandle` that can
    /// be used to abort the task.
    pub fn start_cleanup_task(
        self: &Arc<Self>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                manager.cleanup_expired_messages().await;
            }
        })
    }

    // --- Channel Metadata & TTL ---

    /// Create a channel with explicit configuration.
    /// Returns `true` if the channel was created, `false` if it already exists.
    pub async fn create_channel(&self, name: &str, config: ChannelConfig) -> bool {
        let mut channels = self.channels.write().await;
        if channels.contains_key(name) {
            return false;
        }

        if self.config.max_channels > 0 && channels.len() >= self.config.max_channels {
            warn!(name, "Cannot create channel: max channels limit reached");
            return false;
        }

        let capacity = self.config.channel_capacity;
        let history_size = config.history_size;
        let state = Arc::new(ChannelState::with_options(
            capacity,
            history_size,
            Some(config),
        ));
        channels.insert(name.to_string(), state);
        info!(name, "Created pub/sub channel with custom config");
        true
    }

    /// Remove channels that have been idle (no publish or subscribe activity)
    /// longer than `idle_timeout_ms`. Also removes channels whose
    /// `ChannelConfig.idle_timeout_ms` has been exceeded.
    pub async fn cleanup_idle_channels(&self, default_idle_timeout_ms: u64) -> usize {
        let now = now_millis();
        let mut channels = self.channels.write().await;
        let before = channels.len();
        channels.retain(|name, state| {
            let timeout = state
                .channel_config
                .as_ref()
                .map(|c| c.idle_timeout_ms)
                .filter(|&t| t > 0)
                .unwrap_or(default_idle_timeout_ms);
            if timeout == 0 {
                return true; // No expiry configured
            }
            let last = state.last_activity.load(Ordering::Relaxed);
            let idle = now.saturating_sub(last);
            if idle > timeout && state.subscriber_count() == 0 {
                debug!(channel = %name, idle_ms = idle, "Expiring idle channel");
                false
            } else {
                true
            }
        });
        let removed = before - channels.len();
        if removed > 0 {
            info!(removed, "Cleaned up idle pub/sub channels");
        }
        removed
    }

    // --- Enhanced Pattern Matching ---

    /// Subscribe to all channels matching a glob pattern.
    /// Supports `*` (matches any sequence of characters) and `?` (matches
    /// exactly one character).
    pub async fn subscribe_pattern(&self, pattern: &str) -> broadcast::Receiver<Message> {
        let pattern_subs = self.pattern_subs.read().await;
        if let Some(sender) = pattern_subs.get(pattern) {
            self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
            debug!(pattern, "New subscriber added to existing pattern");
            return sender.subscribe();
        }
        drop(pattern_subs);

        let mut pattern_subs = self.pattern_subs.write().await;
        // Double-check after acquiring write lock
        if let Some(sender) = pattern_subs.get(pattern) {
            self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
            return sender.subscribe();
        }

        let (tx, rx) = broadcast::channel(self.config.channel_capacity);
        pattern_subs.insert(pattern.to_string(), tx);
        self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
        info!(pattern, "Created new pattern subscription");
        rx
    }

    /// Unsubscribe a pattern (removes the pattern entirely).
    /// Returns `true` if the pattern existed.
    pub async fn unsubscribe_pattern(&self, pattern: &str) -> bool {
        let mut pattern_subs = self.pattern_subs.write().await;
        let removed = pattern_subs.remove(pattern).is_some();
        if removed {
            info!(pattern, "Removed pattern subscription");
        }
        removed
    }

    /// Total number of active channels.
    pub async fn channel_count(&self) -> usize {
        let channels = self.channels.read().await;
        channels.len()
    }

    /// Total number of active pattern subscriptions.
    pub async fn pattern_count(&self) -> usize {
        let pattern_subs = self.pattern_subs.read().await;
        pattern_subs.len()
    }

    /// Fan out a message to all pattern subscribers whose pattern matches the channel.
    async fn fan_out_to_patterns(&self, channel: &str, message: &Message) -> usize {
        let pattern_subs = self.pattern_subs.read().await;
        if pattern_subs.is_empty() {
            return 0;
        }
        let mut total = 0;
        for (pattern, sender) in pattern_subs.iter() {
            if matches_pattern(pattern, channel) {
                total += sender.send(message.clone()).unwrap_or(0);
            }
        }
        total
    }
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a persistent subscription's state.
#[derive(Debug, Clone)]
pub struct PersistentSubscription {
    /// Unique subscriber identifier
    pub subscriber_id: String,
    /// Channels this subscriber is tracking
    pub channels: Vec<String>,
    /// Last acknowledged message ID per channel
    pub last_seen: HashMap<String, u64>,
    /// Timestamp when the subscription was first created (Unix millis)
    pub created_at: u64,
}

/// Glob-style pattern matching supporting `*` (any sequence) and `?` (single char).
fn matches_pattern(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (plen, tlen) = (p.len(), t.len());

    // DP approach: dp[i][j] = pattern[0..i] matches text[0..j]
    let mut dp = vec![vec![false; tlen + 1]; plen + 1];
    dp[0][0] = true;

    // Leading `*` can match empty text
    for i in 1..=plen {
        if p[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=plen {
        for j in 1..=tlen {
            match p[i - 1] {
                '*' => dp[i][j] = dp[i - 1][j] || dp[i][j - 1],
                '?' => dp[i][j] = dp[i - 1][j - 1],
                c => dp[i][j] = dp[i - 1][j - 1] && c == t[j - 1],
            }
        }
    }

    dp[plen][tlen]
}

/// Overall pub/sub statistics
#[derive(Debug, Clone)]
pub struct PubSubStats {
    /// Statistics for each channel
    pub channels: Vec<ChannelStats>,
    /// Total number of channels
    pub total_channels: usize,
    /// Total number of active subscribers
    pub total_subscribers: usize,
    /// Total messages published across all channels
    pub total_messages: u64,
    /// Total subscriptions created (may be > current subscribers)
    pub total_subscriptions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_pubsub_basic() {
        let manager = PubSubManager::new();

        // Subscribe to a channel
        let mut receiver = manager.subscribe("test").await;

        // Publish a message
        let count = manager.publish("test", "hello").await;
        assert_eq!(count, 1);

        // Receive the message
        let msg = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("timeout")
            .expect("recv error");

        assert_eq!(msg.channel, "test");
        assert_eq!(msg.payload, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_pubsub_multiple_subscribers() {
        let manager = PubSubManager::new();

        // Create multiple subscribers
        let mut rx1 = manager.subscribe("events").await;
        let mut rx2 = manager.subscribe("events").await;
        let mut rx3 = manager.subscribe("events").await;

        // Publish a message
        let count = manager.publish("events", "broadcast").await;
        assert_eq!(count, 3);

        // All subscribers should receive the message
        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let msg = timeout(Duration::from_millis(100), rx.recv())
                .await
                .expect("timeout")
                .expect("recv error");
            assert_eq!(msg.payload, Bytes::from("broadcast"));
        }
    }

    #[tokio::test]
    async fn test_pubsub_multiple_channels() {
        let manager = PubSubManager::new();

        let mut rx_events = manager.subscribe("events").await;
        let mut rx_alerts = manager.subscribe("alerts").await;

        // Publish to different channels
        manager.publish("events", "event1").await;
        manager.publish("alerts", "alert1").await;

        // Each subscriber receives only their channel's messages
        let event_msg = timeout(Duration::from_millis(100), rx_events.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(event_msg.channel, "events");
        assert_eq!(event_msg.payload, Bytes::from("event1"));

        let alert_msg = timeout(Duration::from_millis(100), rx_alerts.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(alert_msg.channel, "alerts");
        assert_eq!(alert_msg.payload, Bytes::from("alert1"));
    }

    #[tokio::test]
    async fn test_pubsub_no_subscribers() {
        let manager = PubSubManager::new();

        // Publish without subscribers
        let count = manager.publish("empty", "message").await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_pubsub_subscriber_count() {
        let manager = PubSubManager::new();

        assert_eq!(manager.subscriber_count("test").await, 0);

        let _rx1 = manager.subscribe("test").await;
        assert_eq!(manager.subscriber_count("test").await, 1);

        let _rx2 = manager.subscribe("test").await;
        assert_eq!(manager.subscriber_count("test").await, 2);

        // When receivers are dropped, count decreases
        drop(_rx1);
        // Need a small delay for broadcast to notice
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.subscriber_count("test").await, 1);
    }

    #[tokio::test]
    async fn test_pubsub_list_channels() {
        let manager = PubSubManager::new();

        manager.subscribe("channel1").await;
        manager.subscribe("channel2").await;
        manager.subscribe("channel3").await;

        let channels = manager.list_channels().await;
        assert_eq!(channels.len(), 3);
        assert!(channels.contains(&"channel1".to_string()));
        assert!(channels.contains(&"channel2".to_string()));
        assert!(channels.contains(&"channel3".to_string()));
    }

    #[tokio::test]
    async fn test_pubsub_channel_stats() {
        let manager = PubSubManager::new();

        let _rx = manager.subscribe("stats-test").await;
        manager.publish("stats-test", "msg1").await;
        manager.publish("stats-test", "msg2").await;

        let stats = manager.channel_stats("stats-test").await.unwrap();
        assert_eq!(stats.name, "stats-test");
        assert_eq!(stats.subscribers, 1);
        assert_eq!(stats.messages_published, 2);
    }

    #[tokio::test]
    async fn test_pubsub_all_stats() {
        let manager = PubSubManager::new();

        let _rx1 = manager.subscribe("ch1").await;
        let _rx2 = manager.subscribe("ch2").await;
        let _rx3 = manager.subscribe("ch2").await;

        manager.publish("ch1", "msg").await;
        manager.publish("ch2", "msg").await;

        let stats = manager.all_stats().await;
        assert_eq!(stats.total_channels, 2);
        assert_eq!(stats.total_subscribers, 3);
        assert_eq!(stats.total_messages, 2);
        assert_eq!(stats.total_subscriptions, 3);
    }

    #[tokio::test]
    async fn test_pubsub_remove_channel() {
        let manager = PubSubManager::new();

        let mut rx = manager.subscribe("removable").await;
        manager.publish("removable", "msg1").await;

        // Remove the channel
        assert!(manager.remove_channel("removable").await);
        assert!(!manager.remove_channel("removable").await); // Already removed

        // Subscriber should be disconnected
        // Next recv should fail since sender is dropped
        let result = rx.recv().await;
        // First message should still be received
        assert!(result.is_ok());

        // But after that, channel is gone
        let result = rx.recv().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pubsub_cleanup_empty() {
        let manager = PubSubManager::new();

        // Create channels
        let rx1 = manager.subscribe("keep").await;
        let rx2 = manager.subscribe("remove").await;

        // Drop one subscriber
        drop(rx2);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cleanup
        let removed = manager.cleanup_empty_channels().await;
        assert_eq!(removed, 1);

        // Only "keep" should remain
        let channels = manager.list_channels().await;
        assert_eq!(channels.len(), 1);
        assert!(channels.contains(&"keep".to_string()));

        drop(rx1);
    }

    #[tokio::test]
    async fn test_pubsub_config_auto_create_disabled() {
        let config = PubSubConfig {
            auto_create_channels: false,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);

        // Publishing to non-existent channel should return 0
        let count = manager.publish("nonexistent", "msg").await;
        assert_eq!(count, 0);

        // Channel should not exist
        let channels = manager.list_channels().await;
        assert!(channels.is_empty());
    }

    #[tokio::test]
    async fn test_pubsub_config_max_channels() {
        let config = PubSubConfig {
            max_channels: 2,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);

        manager.subscribe("ch1").await;
        manager.subscribe("ch2").await;

        // Third channel should not be created
        let _rx = manager.subscribe("ch3").await;

        let channels = manager.list_channels().await;
        assert_eq!(channels.len(), 2);
        assert!(!channels.contains(&"ch3".to_string()));
    }

    #[tokio::test]
    async fn test_pubsub_subscribe_multiple() {
        let manager = PubSubManager::new();

        let subscriptions = manager.subscribe_multiple(&["a", "b", "c"]).await;
        assert_eq!(subscriptions.len(), 3);

        // Publish to each channel
        manager.publish("a", "msg-a").await;
        manager.publish("b", "msg-b").await;
        manager.publish("c", "msg-c").await;

        // Each receiver should get its channel's message
        for (channel, mut rx) in subscriptions {
            let msg = timeout(Duration::from_millis(100), rx.recv())
                .await
                .expect("timeout")
                .expect("recv error");
            assert_eq!(msg.channel, channel);
        }
    }

    #[tokio::test]
    async fn test_pubsub_publish_multiple() {
        let manager = PubSubManager::new();

        let mut rx_a = manager.subscribe("a").await;
        let mut rx_b = manager.subscribe("b").await;

        let results = manager.publish_multiple(&["a", "b"], "broadcast").await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(_, count)| *count == 1));

        // Both should receive the message
        let msg_a = timeout(Duration::from_millis(100), rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        let msg_b = timeout(Duration::from_millis(100), rx_b.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(msg_a.payload, Bytes::from("broadcast"));
        assert_eq!(msg_b.payload, Bytes::from("broadcast"));
    }

    #[tokio::test]
    async fn test_message_timestamp() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let msg = Message::new("test", "payload");

        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        assert!(msg.timestamp >= before);
        assert!(msg.timestamp <= after);
    }

    // ── Message History Tests ──

    #[tokio::test]
    async fn test_history_disabled_by_default() {
        let manager = PubSubManager::new();
        let _rx = manager.subscribe("h").await;
        manager.publish("h", "msg1").await;
        manager.publish("h", "msg2").await;
        let history = manager.get_history("h", 10).await;
        assert!(
            history.is_empty(),
            "history should be empty when history_size=0"
        );
    }

    #[tokio::test]
    async fn test_history_basic() {
        let config = PubSubConfig {
            history_size: 5,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("h").await;

        for i in 0..3 {
            manager.publish("h", format!("msg{i}")).await;
        }

        let history = manager.get_history("h", 10).await;
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].payload, Bytes::from("msg0"));
        assert_eq!(history[2].payload, Bytes::from("msg2"));
    }

    #[tokio::test]
    async fn test_history_ring_buffer_eviction() {
        let config = PubSubConfig {
            history_size: 3,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("ring").await;

        for i in 0..6 {
            manager.publish("ring", format!("m{i}")).await;
        }

        let history = manager.get_history("ring", 10).await;
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].payload, Bytes::from("m3"));
        assert_eq!(history[1].payload, Bytes::from("m4"));
        assert_eq!(history[2].payload, Bytes::from("m5"));
    }

    #[tokio::test]
    async fn test_history_partial_retrieval() {
        let config = PubSubConfig {
            history_size: 10,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("partial").await;

        for i in 0..5 {
            manager.publish("partial", format!("m{i}")).await;
        }

        let history = manager.get_history("partial", 2).await;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].payload, Bytes::from("m3"));
        assert_eq!(history[1].payload, Bytes::from("m4"));
    }

    #[tokio::test]
    async fn test_subscribe_with_history() {
        let config = PubSubConfig {
            history_size: 10,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("swh").await;

        manager.publish("swh", "old1").await;
        manager.publish("swh", "old2").await;

        let (history, _rx2) = manager.subscribe_with_history("swh", 5).await;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].payload, Bytes::from("old1"));
    }

    // ── Persistent Subscription Tests ──

    #[tokio::test]
    async fn test_persistent_subscribe_new() {
        let config = PubSubConfig {
            history_size: 10,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("p").await;

        manager.publish("p", "a").await;
        manager.publish("p", "b").await;

        // New persistent subscriber should replay all history
        let (replay, _rx2) = manager.subscribe_persistent("sub1", "p").await;
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].payload, Bytes::from("a"));
        assert_eq!(replay[1].payload, Bytes::from("b"));
    }

    #[tokio::test]
    async fn test_persistent_acknowledge_and_resume() {
        let config = PubSubConfig {
            history_size: 20,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("pa").await;

        manager.publish("pa", "m1").await;
        manager.publish("pa", "m2").await;
        manager.publish("pa", "m3").await;

        // First connect: get all 3
        let (replay, _rx2) = manager.subscribe_persistent("sub1", "pa").await;
        assert_eq!(replay.len(), 3);

        // Acknowledge up to second message
        let ack_id = replay[1].id;
        assert!(manager.acknowledge("sub1", "pa", ack_id).await);

        // Disconnect and reconnect: should only replay m3
        drop(_rx2);
        let (replay2, _rx3) = manager.subscribe_persistent("sub1", "pa").await;
        assert_eq!(replay2.len(), 1);
        assert_eq!(replay2[0].payload, Bytes::from("m3"));
    }

    #[tokio::test]
    async fn test_persistent_subscription_state() {
        let config = PubSubConfig {
            history_size: 5,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("ps").await;
        manager.publish("ps", "x").await;

        let (_replay, _rx2) = manager.subscribe_persistent("viewer", "ps").await;

        let sub = manager.get_persistent_subscription("viewer").await;
        assert!(sub.is_some());
        let sub = sub.unwrap();
        assert_eq!(sub.subscriber_id, "viewer");
        assert!(sub.channels.contains(&"ps".to_string()));

        assert!(manager
            .get_persistent_subscription("nonexistent")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_acknowledge_nonexistent_subscriber() {
        let manager = PubSubManager::new();
        assert!(!manager.acknowledge("ghost", "ch", 1).await);
    }

    // ── Channel Metadata & TTL Tests ──

    #[tokio::test]
    async fn test_create_channel_with_config() {
        let manager = PubSubManager::new();

        let created = manager
            .create_channel(
                "custom",
                ChannelConfig {
                    max_subscribers: 10,
                    message_ttl_ms: 5000,
                    history_size: 50,
                    idle_timeout_ms: 60_000,
                },
            )
            .await;
        assert!(created);

        // Creating the same channel again should return false
        assert!(
            !manager
                .create_channel("custom", ChannelConfig::default())
                .await
        );

        // Channel should be listed
        let channels = manager.list_channels().await;
        assert!(channels.contains(&"custom".to_string()));
    }

    #[tokio::test]
    async fn test_create_channel_respects_max() {
        let config = PubSubConfig {
            max_channels: 1,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);

        assert!(
            manager
                .create_channel("first", ChannelConfig::default())
                .await
        );
        assert!(
            !manager
                .create_channel("second", ChannelConfig::default())
                .await
        );
    }

    #[tokio::test]
    async fn test_create_channel_history() {
        let manager = PubSubManager::new();
        manager
            .create_channel(
                "hist",
                ChannelConfig {
                    history_size: 3,
                    ..Default::default()
                },
            )
            .await;

        // Publish without subscribers — history should still be recorded
        manager.publish("hist", "a").await;
        manager.publish("hist", "b").await;

        let history = manager.get_history("hist", 10).await;
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn test_cleanup_idle_channels() {
        let manager = PubSubManager::new();
        manager
            .create_channel("idle-ch", ChannelConfig::default())
            .await;

        // Artificially age the channel by setting last_activity far in the past
        {
            let channels = manager.channels.read().await;
            if let Some(state) = channels.get("idle-ch") {
                state.last_activity.store(1_000, Ordering::Relaxed);
            }
        }

        // Cleanup with a 1ms idle timeout
        let removed = manager.cleanup_idle_channels(1).await;
        assert_eq!(removed, 1);
        assert_eq!(manager.channel_count().await, 0);
    }

    #[tokio::test]
    async fn test_cleanup_idle_channels_keeps_active() {
        let manager = PubSubManager::new();
        let _rx = manager.subscribe("active").await;
        manager.publish("active", "hi").await;

        // Should not remove channels with subscribers
        let removed = manager.cleanup_idle_channels(1).await;
        assert_eq!(removed, 0);
    }

    // ── Enhanced Pattern Matching Tests ──

    #[test]
    fn test_matches_pattern_star() {
        assert!(matches_pattern("events.*", "events.click"));
        assert!(matches_pattern("events.*", "events."));
        assert!(!matches_pattern("events.*", "events"));
        assert!(matches_pattern("*", "anything"));
        assert!(matches_pattern("*.*", "a.b"));
        assert!(!matches_pattern("*.*", "nope"));
    }

    #[test]
    fn test_matches_pattern_question() {
        assert!(matches_pattern("event?", "events"));
        assert!(!matches_pattern("event?", "event"));
        assert!(!matches_pattern("event?", "eventss"));
        assert!(matches_pattern("a?c", "abc"));
        assert!(!matches_pattern("a?c", "ac"));
    }

    #[test]
    fn test_matches_pattern_combined() {
        assert!(matches_pattern("us?r.*", "user.created"));
        assert!(matches_pattern("us?r.*", "usar.deleted"));
        assert!(!matches_pattern("us?r.*", "user"));
        assert!(matches_pattern("*?*", "ab"));
        assert!(matches_pattern("*?*", "a"));
        assert!(!matches_pattern("*?*", ""));
    }

    #[test]
    fn test_matches_pattern_exact() {
        assert!(matches_pattern("exact", "exact"));
        assert!(!matches_pattern("exact", "exactl"));
        assert!(!matches_pattern("exact", "exac"));
    }

    #[tokio::test]
    async fn test_subscribe_pattern_basic() {
        let manager = PubSubManager::new();

        let mut rx = manager.subscribe_pattern("events.*").await;

        manager.publish("events.click", "click!").await;
        manager.publish("events.scroll", "scroll!").await;
        manager.publish("other.thing", "nope").await;

        let msg1 = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(msg1.channel, "events.click");

        let msg2 = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(msg2.channel, "events.scroll");
    }

    #[tokio::test]
    async fn test_subscribe_pattern_question_mark() {
        let manager = PubSubManager::new();

        let mut rx = manager.subscribe_pattern("log?").await;

        manager.publish("logs", "yes").await;
        manager.publish("logi", "yes2").await;
        manager.publish("log", "no").await;
        manager.publish("logss", "no2").await;

        let msg1 = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(msg1.channel, "logs");

        let msg2 = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(msg2.channel, "logi");
    }

    #[tokio::test]
    async fn test_pattern_count_and_channel_count() {
        let manager = PubSubManager::new();

        assert_eq!(manager.channel_count().await, 0);
        assert_eq!(manager.pattern_count().await, 0);

        let _rx1 = manager.subscribe("ch1").await;
        let _rx2 = manager.subscribe("ch2").await;
        assert_eq!(manager.channel_count().await, 2);

        let _prx1 = manager.subscribe_pattern("evt.*").await;
        let _prx2 = manager.subscribe_pattern("log?").await;
        assert_eq!(manager.pattern_count().await, 2);
    }

    #[tokio::test]
    async fn test_unsubscribe_pattern() {
        let manager = PubSubManager::new();

        let _rx = manager.subscribe_pattern("a.*").await;
        assert_eq!(manager.pattern_count().await, 1);

        assert!(manager.unsubscribe_pattern("a.*").await);
        assert_eq!(manager.pattern_count().await, 0);

        assert!(!manager.unsubscribe_pattern("nonexistent").await);
    }

    // ── Message ID Tests ──

    #[tokio::test]
    async fn test_message_ids_are_monotonic() {
        let config = PubSubConfig {
            history_size: 10,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("ids").await;

        manager.publish("ids", "a").await;
        manager.publish("ids", "b").await;
        manager.publish("ids", "c").await;

        let history = manager.get_history("ids", 10).await;
        assert_eq!(history.len(), 3);
        assert!(history[0].id < history[1].id);
        assert!(history[1].id < history[2].id);
        assert!(history[0].id > 0);
    }

    // ── TTL Cleanup Tests ──

    #[tokio::test]
    async fn test_cleanup_expired_messages_removes_old() {
        let config = PubSubConfig {
            history_size: 100,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);

        // Create channel with a 50ms TTL
        let ch_config = ChannelConfig {
            message_ttl_ms: 50,
            history_size: 100,
            ..Default::default()
        };
        manager.create_channel("ttl-ch", ch_config).await;
        let _rx = manager.subscribe("ttl-ch").await;

        // Publish messages
        manager.publish("ttl-ch", "old1").await;
        manager.publish("ttl-ch", "old2").await;

        // Wait for messages to expire
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Publish a fresh message
        manager.publish("ttl-ch", "fresh").await;

        // Run cleanup
        let removed = manager.cleanup_expired_messages().await;
        assert_eq!(removed, 2, "should have removed 2 expired messages");

        // Verify only the fresh message remains
        let history = manager.get_history("ttl-ch", 100).await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].payload, Bytes::from("fresh"));
    }

    #[tokio::test]
    async fn test_cleanup_expired_messages_skips_no_ttl() {
        let config = PubSubConfig {
            history_size: 100,
            ..Default::default()
        };
        let manager = PubSubManager::with_config(config);
        let _rx = manager.subscribe("no-ttl").await;

        manager.publish("no-ttl", "msg1").await;
        manager.publish("no-ttl", "msg2").await;

        // No TTL configured — cleanup should remove nothing
        let removed = manager.cleanup_expired_messages().await;
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn test_start_cleanup_task_runs() {
        let config = PubSubConfig {
            history_size: 100,
            ..Default::default()
        };
        let manager = Arc::new(PubSubManager::with_config(config));

        let ch_config = ChannelConfig {
            message_ttl_ms: 30,
            history_size: 100,
            ..Default::default()
        };
        manager.create_channel("task-ch", ch_config).await;
        let _rx = manager.subscribe("task-ch").await;

        manager.publish("task-ch", "will-expire").await;

        // Start cleanup with a short interval
        let handle = manager.start_cleanup_task(Duration::from_millis(20));

        // Wait enough for messages to expire and cleanup to run
        tokio::time::sleep(Duration::from_millis(120)).await;

        handle.abort();

        let history = manager.get_history("task-ch", 100).await;
        assert_eq!(
            history.len(),
            0,
            "expired messages should have been cleaned up by the task"
        );
    }

    // ── Persistent Subscription Concurrency Tests ──

    #[tokio::test]
    async fn test_persistent_subscribe_concurrent_access() {
        let config = PubSubConfig {
            history_size: 100,
            ..Default::default()
        };
        let manager = Arc::new(PubSubManager::with_config(config));
        let _rx = manager.subscribe("conc-ch").await;

        // Publish some messages so there's history to replay
        for i in 0..10 {
            manager.publish("conc-ch", format!("msg-{i}")).await;
        }

        // Spawn multiple concurrent persistent subscribers with the same ID
        let mut handles = Vec::new();
        for _ in 0..10 {
            let mgr = Arc::clone(&manager);
            handles.push(tokio::spawn(async move {
                let (replay, _rx) = mgr.subscribe_persistent("sub-1", "conc-ch").await;
                replay.len()
            }));
        }

        let mut replay_counts = Vec::new();
        for h in handles {
            replay_counts.push(h.await.unwrap());
        }

        // All should get a consistent replay (10 messages the first time,
        // possibly 0 if another task acknowledged — but no panics or data corruption)
        for count in &replay_counts {
            assert!(*count <= 10, "replay count should be <= 10, got {count}");
        }

        // Verify subscription state is consistent
        let sub = manager.get_persistent_subscription("sub-1").await;
        assert!(sub.is_some());
        let sub = sub.unwrap();
        assert!(sub.channels.contains(&"conc-ch".to_string()));
    }

    #[tokio::test]
    async fn test_concurrent_acknowledge_is_safe() {
        let config = PubSubConfig {
            history_size: 100,
            ..Default::default()
        };
        let manager = Arc::new(PubSubManager::with_config(config));
        let _rx = manager.subscribe("ack-ch").await;

        // Set up persistent subscription
        manager.subscribe_persistent("acker", "ack-ch").await;

        // Publish messages
        for _ in 0..20 {
            manager.publish("ack-ch", "data").await;
        }

        // Concurrent acknowledgments — each acking a different message ID
        let mut handles = Vec::new();
        for i in 1..=20u64 {
            let mgr = Arc::clone(&manager);
            handles.push(tokio::spawn(async move {
                mgr.acknowledge("acker", "ack-ch", i).await
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // The highest acked offset should be 20
        let sub = manager.get_persistent_subscription("acker").await.unwrap();
        assert_eq!(*sub.last_seen.get("ack-ch").unwrap(), 20);
    }
}
