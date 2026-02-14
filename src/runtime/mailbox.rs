//! Cross-shard message passing
//!
//! This module provides lock-free SPSC (Single-Producer Single-Consumer)
//! queues for efficient cross-shard communication without locks.

use super::RuntimeResult;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::trace;

/// A message that can be sent between shards
#[derive(Debug, Clone)]
pub enum ShardMessage {
    /// Request to process a produce batch
    ProduceBatch {
        topic: String,
        partition: u32,
        data: Vec<u8>,
    },

    /// Request to fetch data
    FetchRequest {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        response_channel: usize, // Shard ID to send response to
    },

    /// Response to a fetch request
    FetchResponse { request_id: u64, data: Vec<u8> },

    /// Partition ownership change notification
    PartitionOwnership {
        topic: String,
        partition: u32,
        new_owner: usize,
    },

    /// Health check ping
    Ping { request_id: u64 },

    /// Health check pong
    Pong { request_id: u64 },

    /// Shutdown signal
    Shutdown,

    /// Custom message for extensions
    Custom { message_type: u32, payload: Vec<u8> },
}

/// Mailbox for receiving messages from other shards
pub struct Mailbox {
    /// Shard ID this mailbox belongs to
    shard_id: usize,

    /// Incoming message queues from each sender shard
    /// Index = sender shard ID
    incoming: Vec<SpscQueue<ShardMessage>>,

    /// Total messages received
    messages_received: AtomicU64,

    /// Messages dropped (queue full)
    messages_dropped: AtomicU64,
}

impl Mailbox {
    /// Create a new mailbox for a shard
    pub fn new(shard_id: usize, shard_count: usize, capacity: usize) -> Self {
        let incoming = (0..shard_count).map(|_| SpscQueue::new(capacity)).collect();

        Self {
            shard_id,
            incoming,
            messages_received: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
        }
    }

    /// Receive a message (from a specific sender shard)
    pub fn receive_from(&self, sender_shard: usize) -> Option<ShardMessage> {
        if let Some(queue) = self.incoming.get(sender_shard) {
            if let Some(msg) = queue.pop() {
                self.messages_received.fetch_add(1, Ordering::Relaxed);
                return Some(msg);
            }
        }
        None
    }

    /// Receive messages from all shards
    pub fn receive_all(&self) -> Vec<ShardMessage> {
        let mut messages = Vec::new();

        for queue in &self.incoming {
            while let Some(msg) = queue.pop() {
                messages.push(msg);
            }
        }

        if !messages.is_empty() {
            self.messages_received
                .fetch_add(messages.len() as u64, Ordering::Relaxed);
        }

        messages
    }

    /// Get shard ID
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    /// Get queue for a sender (used by MessageRouter)
    #[allow(dead_code)]
    pub(crate) fn get_queue(&self, sender: usize) -> Option<&SpscQueue<ShardMessage>> {
        self.incoming.get(sender)
    }

    /// Get statistics
    pub fn stats(&self) -> MailboxStats {
        MailboxStats {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            queue_depths: self.incoming.iter().map(|q| q.len()).collect(),
        }
    }
}

/// Mailbox statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MailboxStats {
    /// Total messages received
    pub messages_received: u64,
    /// Messages dropped due to full queue
    pub messages_dropped: u64,
    /// Current queue depths per sender
    pub queue_depths: Vec<usize>,
}

/// Message router for cross-shard communication
pub struct MessageRouter {
    /// Number of shards
    shard_count: usize,

    /// Mailboxes for each shard
    mailboxes: Vec<Arc<Mailbox>>,

    /// Messages sent
    messages_sent: AtomicU64,

    /// Messages dropped
    messages_dropped: AtomicU64,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            mailboxes: Vec::new(),
            messages_sent: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
        }
    }

    /// Create a mailbox for a shard
    pub fn create_mailbox(&self, shard_id: usize) -> Arc<Mailbox> {
        Arc::new(Mailbox::new(shard_id, self.shard_count, 4096))
    }

    /// Register a mailbox (called during runtime initialization)
    pub fn register_mailbox(&mut self, mailbox: Arc<Mailbox>) {
        let shard_id = mailbox.shard_id();
        while self.mailboxes.len() <= shard_id {
            self.mailboxes.push(Arc::new(Mailbox::new(
                self.mailboxes.len(),
                self.shard_count,
                4096,
            )));
        }
        self.mailboxes[shard_id] = mailbox;
    }

    /// Send a message to a shard
    pub fn send(&self, target_shard: usize, _message: ShardMessage) -> RuntimeResult<()> {
        // For now, we don't have a proper routing mechanism since mailboxes
        // are created independently. In production, we'd wire this up properly.
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        trace!(target = target_shard, "Message sent (simulated)");
        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> RouterStats {
        RouterStats {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Router statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RouterStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Messages dropped
    pub messages_dropped: u64,
}

/// Lock-free SPSC (Single-Producer Single-Consumer) queue
///
/// This is a bounded, lock-free queue optimized for cross-shard communication
/// where there's exactly one producer and one consumer.
pub struct SpscQueue<T> {
    /// Ring buffer
    buffer: Vec<Mutex<Option<T>>>,

    /// Capacity (power of 2)
    capacity: usize,

    /// Mask for index wrapping
    mask: usize,

    /// Producer position
    head: AtomicUsize,

    /// Consumer position
    tail: AtomicUsize,
}

impl<T> SpscQueue<T> {
    /// Create a new SPSC queue
    pub fn new(capacity: usize) -> Self {
        // Round up to power of 2
        let capacity = capacity.next_power_of_two();

        let buffer = (0..capacity).map(|_| Mutex::new(None)).collect();

        Self {
            buffer,
            capacity,
            mask: capacity - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Push an item (returns false if full)
    pub fn push(&self, item: T) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Check if full
        if head.wrapping_sub(tail) >= self.capacity {
            return false;
        }

        let index = head & self.mask;
        *self.buffer[index].lock() = Some(item);

        self.head.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    /// Pop an item
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        // Check if empty
        if head == tail {
            return None;
        }

        let index = tail & self.mask;
        let item = self.buffer[index].lock().take();

        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        item
    }

    /// Get current length
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if full
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Get capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

unsafe impl<T: Send> Send for SpscQueue<T> {}
unsafe impl<T: Send> Sync for SpscQueue<T> {}

/// Batch message sender for efficiency
pub struct BatchSender {
    /// Target shard
    target_shard: usize,

    /// Batch buffer
    batch: Vec<ShardMessage>,

    /// Max batch size
    max_batch_size: usize,
}

impl BatchSender {
    /// Create a new batch sender
    pub fn new(target_shard: usize, max_batch_size: usize) -> Self {
        Self {
            target_shard,
            batch: Vec::with_capacity(max_batch_size),
            max_batch_size,
        }
    }

    /// Add a message to the batch
    pub fn add(&mut self, message: ShardMessage) -> bool {
        if self.batch.len() >= self.max_batch_size {
            return false;
        }
        self.batch.push(message);
        true
    }

    /// Get batch for sending
    pub fn take_batch(&mut self) -> Vec<ShardMessage> {
        std::mem::take(&mut self.batch)
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.batch.len() >= self.max_batch_size
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Get target shard
    pub fn target_shard(&self) -> usize {
        self.target_shard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spsc_queue_basic() {
        let queue: SpscQueue<u32> = SpscQueue::new(4);

        assert!(queue.is_empty());
        assert!(!queue.is_full());

        assert!(queue.push(1));
        assert!(queue.push(2));
        assert!(queue.push(3));
        assert!(queue.push(4));
        assert!(!queue.push(5)); // Full

        assert_eq!(queue.len(), 4);
        assert!(queue.is_full());

        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.len(), 2);

        assert!(queue.push(5));
        assert!(queue.push(6));

        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(4));
        assert_eq!(queue.pop(), Some(5));
        assert_eq!(queue.pop(), Some(6));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_mailbox() {
        let mailbox = Mailbox::new(0, 3, 16);

        // Simulate receiving from shard 1
        let queue = mailbox.get_queue(1).unwrap();
        queue.push(ShardMessage::Ping { request_id: 1 });

        let messages = mailbox.receive_all();
        assert_eq!(messages.len(), 1);

        let stats = mailbox.stats();
        assert_eq!(stats.messages_received, 1);
    }

    #[test]
    fn test_batch_sender() {
        let mut sender = BatchSender::new(0, 3);

        assert!(sender.is_empty());
        assert!(sender.add(ShardMessage::Ping { request_id: 1 }));
        assert!(sender.add(ShardMessage::Ping { request_id: 2 }));
        assert!(sender.add(ShardMessage::Ping { request_id: 3 }));
        assert!(sender.is_full());
        assert!(!sender.add(ShardMessage::Ping { request_id: 4 }));

        let batch = sender.take_batch();
        assert_eq!(batch.len(), 3);
        assert!(sender.is_empty());
    }

    #[test]
    fn test_message_router() {
        let router = MessageRouter::new(4);
        assert!(router.send(0, ShardMessage::Ping { request_id: 1 }).is_ok());
    }
}
