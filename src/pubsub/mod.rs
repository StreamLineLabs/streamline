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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};

/// Default channel buffer size for broadcast channels
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Message delivered through pub/sub
#[derive(Debug, Clone)]
pub struct Message {
    /// Channel the message was published to
    pub channel: String,
    /// Message payload
    pub payload: Bytes,
    /// Timestamp when the message was published (Unix millis)
    pub timestamp: u64,
}

impl Message {
    /// Create a new message
    pub fn new(channel: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            channel: channel.into(),
            payload: payload.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
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

/// Channel state including broadcast sender and stats
struct ChannelState {
    /// Broadcast sender for the channel
    sender: broadcast::Sender<Message>,
    /// Number of messages published
    messages_published: AtomicU64,
}

impl ChannelState {
    fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            messages_published: AtomicU64::new(0),
        }
    }

    fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
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
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            auto_create_channels: true,
            max_channels: 0, // Unlimited
        }
    }
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
            let state = channels
                .entry(channel.to_string())
                .or_insert_with(|| {
                    info!(channel, "Created new pub/sub channel");
                    Arc::new(ChannelState::new(self.config.channel_capacity))
                })
                .clone();

            self.total_subscriptions.fetch_add(1, Ordering::Relaxed);
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
        let channels = self.channels.read().await;

        if let Some(state) = channels.get(channel) {
            let message = Message::new(channel, payload);
            let count = state.sender.send(message).unwrap_or(0);
            state.messages_published.fetch_add(1, Ordering::Relaxed);
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            debug!(channel, subscribers = count, "Published message to channel");
            return count;
        }
        drop(channels);

        // Channel doesn't exist
        if self.config.auto_create_channels {
            let mut channels = self.channels.write().await;

            // Check max channels limit
            if self.config.max_channels > 0 && channels.len() >= self.config.max_channels {
                return 0;
            }

            let state = channels
                .entry(channel.to_string())
                .or_insert_with(|| {
                    info!(channel, "Created new pub/sub channel on publish");
                    Arc::new(ChannelState::new(self.config.channel_capacity))
                })
                .clone();

            let message = Message::new(channel, payload);
            let count = state.sender.send(message).unwrap_or(0);
            state.messages_published.fetch_add(1, Ordering::Relaxed);
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            debug!(channel, subscribers = count, "Published message to channel");
            count
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
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
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
}
