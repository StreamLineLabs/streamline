//! Async embedding worker for semantic topics.
//!
//! The worker runs one task per (topic, partition) and pulls `EmbedJob`s from
//! a bounded channel fed by the append path. Embedding is performed via the
//! configured embedder (candle/ort/etc.); resulting vectors are written into
//! the per-partition `SemanticIndex`.
//!
//! Stability tier: **Experimental**. Not wired into `lib.rs` yet — enable
//! when M2 P1 lands.

use std::sync::Arc;

/// One unit of work for the embedding pipeline.
#[derive(Debug, Clone)]
pub struct EmbedJob {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Bytes to embed. The worker is responsible for decoding (utf-8 or
    /// JSON-field extraction per `TopicConfig.semantic.field`).
    pub payload: Vec<u8>,
}

/// Trait an embedder must implement. Implementations live behind `ai-embed`
/// feature (candle, ort, hosted-API, etc.).
pub trait Embedder: Send + Sync {
    fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>;
    fn dim(&self) -> usize;
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("embedder backend error: {0}")]
    Backend(String),
}

/// Cheaply-cloned handle to a running worker.
#[derive(Clone)]
pub struct EmbedderHandle {
    pub(crate) sender: std::sync::mpsc::SyncSender<EmbedJob>,
}

impl EmbedderHandle {
    /// Try to enqueue a job; returns `false` if the queue is full
    /// (caller should bump a `dropped` metric).
    pub fn try_submit(&self, job: EmbedJob) -> bool {
        self.sender.try_send(job).is_ok()
    }
}

/// Spawnable worker. Runs on its own thread (or task) and drains the queue.
pub struct EmbedWorker<E: Embedder> {
    pub embedder: Arc<E>,
    pub queue_capacity: usize,
}

impl<E: Embedder + 'static> EmbedWorker<E> {
    pub fn new(embedder: Arc<E>, queue_capacity: usize) -> Self {
        Self {
            embedder,
            queue_capacity,
        }
    }

    /// Spawn the worker on a dedicated OS thread; returns a handle to submit
    /// jobs. As jobs land, embeddings are written into the per-topic index
    /// in [`super::registry`].
    pub fn spawn(self) -> EmbedderHandle {
        let (tx, rx) = std::sync::mpsc::sync_channel::<EmbedJob>(self.queue_capacity);
        let embedder = self.embedder;
        std::thread::Builder::new()
            .name("streamline-embed-worker".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    let text = match std::str::from_utf8(&job.payload) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let vector = match embedder.embed(text) {
                        Ok(v) => v,
                        Err(_) => {
                            if let Ok(mut ledger) = super::cost_ledger::global().lock() {
                                ledger.record_error(&job.topic);
                            }
                            continue;
                        }
                    };
                    let idx = super::registry::get_or_create(&job.topic);
                    super::SemanticIndex::insert(&*idx, job.partition, job.offset, &vector);
                    if let Ok(mut ledger) = super::cost_ledger::global().lock() {
                        ledger.record_embed(&job.topic, job.payload.len() as u64, "default");
                    }
                }
            })
            .map(|_handle| ())
            .unwrap_or_else(|e| {
                // The OS refused a thread. Embedding then silently stops rather
                // than taking the whole server down with a panic.
                tracing::error!(error = %e, "failed to spawn embed worker thread");
            });
        EmbedderHandle { sender: tx }
    }
}
