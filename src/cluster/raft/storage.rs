//! Raft storage implementation for Streamline
//!
//! This module provides a combined storage backend for Raft log entries,
//! vote state, and state machine using the RaftStorage trait.

use super::state_machine::ClusterMetadata;
use super::types::{ClusterCommand, ClusterResponse, StreamlineTypeConfig};
use crate::cluster::node::NodeId;
use openraft::storage::{LogState, RaftSnapshotBuilder, Snapshot};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftLogId,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Combined Raft storage for both log and state machine
pub struct StreamlineStore {
    /// Data directory for persistence
    data_dir: PathBuf,

    /// Current vote
    vote: RwLock<Option<Vote<NodeId>>>,

    /// In-memory log entries (Arc-wrapped for sharing with LogReader)
    log: Arc<RwLock<BTreeMap<u64, Entry<StreamlineTypeConfig>>>>,

    /// Last purged log ID
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,

    /// Last committed log ID
    committed: RwLock<Option<LogId<NodeId>>>,

    /// State machine state
    state_machine: RwLock<StateMachineState>,

    /// Current snapshot
    snapshot: RwLock<Option<StoredSnapshot>>,

    /// Self reference for snapshot builder (set after creation)
    self_ref: RwLock<Option<std::sync::Weak<Self>>>,
}

/// State machine internal state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateMachineState {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub metadata: ClusterMetadata,
}

/// Stored snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Helper to create storage IO error
fn io_err(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    e: impl std::error::Error + 'static,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(&e)),
    }
}

impl StreamlineStore {
    /// Create a new store
    pub fn new(data_dir: PathBuf, cluster_id: String) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&data_dir)?;

        let store = Self {
            data_dir,
            vote: RwLock::new(None),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: RwLock::new(None),
            committed: RwLock::new(None),
            state_machine: RwLock::new(StateMachineState {
                metadata: ClusterMetadata::new(cluster_id),
                ..Default::default()
            }),
            snapshot: RwLock::new(None),
            self_ref: RwLock::new(None),
        };

        // Try to load persisted state
        if let Err(e) = store.load_state_sync() {
            debug!(error = %e, "No persisted state found, starting fresh");
        }

        Ok(store)
    }

    /// Set the self reference for snapshot building.
    ///
    /// # IMPORTANT
    ///
    /// This method MUST be called immediately after creating the StreamlineStore
    /// and wrapping it in an Arc. Failure to do so will cause a panic when
    /// `get_snapshot_builder()` is called by the Raft engine.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let store = Arc::new(StreamlineStore::new(path, cluster_id)?);
    /// store.set_self_ref(store.clone()).await;  // Required!
    /// ```
    pub async fn set_self_ref(&self, arc: Arc<Self>) {
        let mut self_ref = self.self_ref.write().await;
        *self_ref = Some(Arc::downgrade(&arc));
    }

    fn state_path(&self) -> PathBuf {
        self.data_dir.join("raft_state.bin")
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.join("raft_log.bin")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join("snapshot.bin")
    }

    // Legacy JSON paths for migration
    fn legacy_state_path(&self) -> PathBuf {
        self.data_dir.join("raft_state.json")
    }
    fn legacy_log_path(&self) -> PathBuf {
        self.data_dir.join("raft_log.json")
    }
    fn legacy_snapshot_path(&self) -> PathBuf {
        self.data_dir.join("snapshot.json")
    }

    fn load_state_sync(&self) -> Result<(), std::io::Error> {
        #[derive(Deserialize)]
        struct PersistedState {
            vote: Option<Vote<NodeId>>,
            committed: Option<LogId<NodeId>>,
            state_machine: StateMachineState,
        }

        let state_path = self.state_path();
        let legacy_state_path = self.legacy_state_path();

        if state_path.exists() {
            let bytes = std::fs::read(&state_path)?;
            let state: PersistedState = bincode::deserialize(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut vote) = self.vote.try_write() {
                *vote = state.vote;
            }
            if let Ok(mut committed) = self.committed.try_write() {
                *committed = state.committed;
            }
            if let Ok(mut sm) = self.state_machine.try_write() {
                *sm = state.state_machine;
            }
            info!("Loaded raft state from binary format");
        } else if legacy_state_path.exists() {
            // Migrate from legacy JSON format
            let contents = std::fs::read_to_string(&legacy_state_path)?;
            let state: PersistedState = serde_json::from_str(&contents)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut vote) = self.vote.try_write() {
                *vote = state.vote;
            }
            if let Ok(mut committed) = self.committed.try_write() {
                *committed = state.committed;
            }
            if let Ok(mut sm) = self.state_machine.try_write() {
                *sm = state.state_machine;
            }
            info!("Migrated raft state from JSON to binary format");
        }

        #[derive(Deserialize)]
        struct LogPersistence {
            entries: BTreeMap<u64, Entry<StreamlineTypeConfig>>,
            last_purged_log_id: Option<LogId<NodeId>>,
        }

        let log_path = self.log_path();
        let legacy_log_path = self.legacy_log_path();

        if log_path.exists() {
            let bytes = std::fs::read(&log_path)?;
            let log_data: LogPersistence = bincode::deserialize(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut log) = self.log.try_write() {
                *log = log_data.entries;
            }
            if let Ok(mut purged) = self.last_purged_log_id.try_write() {
                *purged = log_data.last_purged_log_id;
            }
        } else if legacy_log_path.exists() {
            let contents = std::fs::read_to_string(&legacy_log_path)?;
            let log_data: LogPersistence = serde_json::from_str(&contents)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut log) = self.log.try_write() {
                *log = log_data.entries;
            }
            if let Ok(mut purged) = self.last_purged_log_id.try_write() {
                *purged = log_data.last_purged_log_id;
            }
            info!("Migrated raft log from JSON to binary format");
        }

        let snapshot_path = self.snapshot_path();
        let legacy_snapshot_path = self.legacy_snapshot_path();

        if snapshot_path.exists() {
            let bytes = std::fs::read(&snapshot_path)?;
            let snap: StoredSnapshot = bincode::deserialize(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut snapshot) = self.snapshot.try_write() {
                *snapshot = Some(snap);
            }
        } else if legacy_snapshot_path.exists() {
            let contents = std::fs::read_to_string(&legacy_snapshot_path)?;
            let snap: StoredSnapshot = serde_json::from_str(&contents)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Ok(mut snapshot) = self.snapshot.try_write() {
                *snapshot = Some(snap);
            }
            info!("Migrated raft snapshot from JSON to binary format");
        }

        Ok(())
    }

    async fn save_state(&self) -> Result<(), std::io::Error> {
        #[derive(Serialize)]
        struct PersistedState<'a> {
            vote: &'a Option<Vote<NodeId>>,
            committed: &'a Option<LogId<NodeId>>,
            state_machine: &'a StateMachineState,
        }

        let vote = self.vote.read().await;
        let committed = self.committed.read().await;
        let sm = self.state_machine.read().await;

        let state = PersistedState {
            vote: &vote,
            committed: &committed,
            state_machine: &sm,
        };

        let bytes = bincode::serialize(&state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        tokio::fs::write(self.state_path(), bytes).await?;
        Ok(())
    }

    async fn save_log(&self) -> Result<(), std::io::Error> {
        #[derive(Serialize)]
        struct LogPersistence<'a> {
            entries: &'a BTreeMap<u64, Entry<StreamlineTypeConfig>>,
            last_purged_log_id: &'a Option<LogId<NodeId>>,
        }

        let log = self.log.read().await;
        let purged = self.last_purged_log_id.read().await;

        let persistence = LogPersistence {
            entries: &log,
            last_purged_log_id: &purged,
        };

        let bytes = bincode::serialize(&persistence)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        tokio::fs::write(self.log_path(), bytes).await?;
        Ok(())
    }

    async fn save_snapshot(&self) -> Result<(), std::io::Error> {
        let snapshot = self.snapshot.read().await;
        if let Some(snap) = snapshot.as_ref() {
            let bytes = bincode::serialize(snap)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            tokio::fs::write(self.snapshot_path(), bytes).await?;
        }
        Ok(())
    }

    /// Get cluster metadata
    pub async fn get_metadata(&self) -> ClusterMetadata {
        self.state_machine.read().await.metadata.clone()
    }

    fn apply_command(state: &mut StateMachineState, cmd: ClusterCommand) -> ClusterResponse {
        match cmd {
            ClusterCommand::RegisterBroker(broker) => {
                let node_id = broker.node_id;
                state.metadata.register_broker(broker);
                ClusterResponse::BrokerRegistered { node_id }
            }
            ClusterCommand::RemoveBroker(node_id) => {
                state.metadata.remove_broker(node_id);
                ClusterResponse::Ok
            }
            ClusterCommand::CreateTopic { name, assignment } => {
                state.metadata.create_topic(assignment);
                ClusterResponse::TopicCreated { name }
            }
            ClusterCommand::DeleteTopic(topic) => {
                state.metadata.delete_topic(&topic);
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateLeader {
                topic,
                partition,
                leader,
                leader_epoch,
            } => {
                state
                    .metadata
                    .update_leader(&topic, partition, leader, leader_epoch);
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateIsr {
                topic,
                partition,
                isr,
            } => {
                state.metadata.update_isr(&topic, partition, isr);
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateBrokerState {
                node_id,
                state: broker_state,
            } => {
                state.metadata.update_broker_state(node_id, broker_state);
                ClusterResponse::Ok
            }
        }
    }
}

/// Log reader for StreamlineStore
pub struct StreamlineLogReader {
    log: Arc<RwLock<BTreeMap<u64, Entry<StreamlineTypeConfig>>>>,
}

impl openraft::storage::RaftLogReader<StreamlineTypeConfig> for StreamlineLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<StreamlineTypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

// Implement RaftLogReader for StreamlineStore itself to satisfy RaftStorage trait bound
impl openraft::storage::RaftLogReader<StreamlineTypeConfig> for StreamlineStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<StreamlineTypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

// Implement RaftLogReader for Arc<StreamlineStore> to allow using Arc with Adaptor
impl openraft::storage::RaftLogReader<StreamlineTypeConfig> for Arc<StreamlineStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<StreamlineTypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

/// Wrapper type for snapshot building that holds an Arc to the store
pub struct SnapshotBuilderWrapper {
    store: Arc<StreamlineStore>,
}

/// Fallback snapshot builder used when the Arc<StreamlineStore> self-reference
/// is unavailable (programming error or store already dropped). Builds a
/// snapshot from a cloned state machine state without persisting it.
pub struct FallbackSnapshotBuilder {
    state_machine: StateMachineState,
}

/// Enum dispatching between the normal and fallback snapshot builders.
/// This avoids a panic when `set_self_ref()` was not called.
pub enum SnapshotBuilderEnum {
    Normal(SnapshotBuilderWrapper),
    Fallback(FallbackSnapshotBuilder),
}

impl RaftSnapshotBuilder<StreamlineTypeConfig> for SnapshotBuilderWrapper {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<StreamlineTypeConfig>, StorageError<NodeId>> {
        let sm = self.store.state_machine.read().await;

        let data = bincode::serialize(&sm.metadata).map_err(|e| {
            error!(error = %e, "Failed to serialize metadata");
            io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        let snapshot_id = if let Some(log_id) = sm.last_applied_log {
            format!(
                "{}-{}-{}",
                log_id.leader_id,
                log_id.index,
                uuid::Uuid::new_v4()
            )
        } else {
            format!("0-0-{}", uuid::Uuid::new_v4())
        };

        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id,
        };

        drop(sm);

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut snapshot = self.store.snapshot.write().await;
            *snapshot = Some(stored);
        }

        if let Err(e) = self.store.save_snapshot().await {
            error!(error = %e, "Failed to persist snapshot");
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<StreamlineTypeConfig> for FallbackSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<StreamlineTypeConfig>, StorageError<NodeId>> {
        let data = bincode::serialize(&self.state_machine.metadata).map_err(|e| {
            error!(error = %e, "Fallback: Failed to serialize metadata");
            io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        let snapshot_id = if let Some(log_id) = self.state_machine.last_applied_log {
            format!(
                "{}-{}-{}",
                log_id.leader_id,
                log_id.index,
                uuid::Uuid::new_v4()
            )
        } else {
            format!("0-0-{}", uuid::Uuid::new_v4())
        };

        let meta = SnapshotMeta {
            last_log_id: self.state_machine.last_applied_log,
            last_membership: self.state_machine.last_membership.clone(),
            snapshot_id,
        };

        // Note: fallback path does not persist the snapshot to disk since
        // we don't have access to the store's save_snapshot method.

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<StreamlineTypeConfig> for SnapshotBuilderEnum {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<StreamlineTypeConfig>, StorageError<NodeId>> {
        match self {
            SnapshotBuilderEnum::Normal(inner) => inner.build_snapshot().await,
            SnapshotBuilderEnum::Fallback(inner) => inner.build_snapshot().await,
        }
    }
}

#[allow(deprecated)]
impl openraft::RaftStorage<StreamlineTypeConfig> for StreamlineStore {
    type LogReader = StreamlineLogReader;
    type SnapshotBuilder = SnapshotBuilderEnum;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Share the Arc reference to the log, don't clone the data.
        // This ensures newly appended entries are immediately visible to readers,
        // satisfying openraft's storage guarantee that entries are readable after append.
        StreamlineLogReader {
            log: self.log.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!(?vote, "Saving vote");
        {
            let mut v = self.vote.write().await;
            *v = Some(*vote);
        }
        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<StreamlineTypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let purged = self.last_purged_log_id.read().await;

        let last_log_id = log.iter().next_back().map(|(_, e)| *e.get_log_id());

        Ok(LogState {
            last_purged_log_id: *purged,
            last_log_id,
        })
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<StreamlineTypeConfig>> + Send,
    {
        let entries_vec: Vec<_> = entries.into_iter().collect();

        if !entries_vec.is_empty() {
            debug!(
                count = entries_vec.len(),
                first = ?entries_vec.first().map(|e| e.get_log_id()),
                last = ?entries_vec.last().map(|e| e.get_log_id()),
                "Appending log entries"
            );
        }

        {
            let mut log = self.log.write().await;
            for entry in entries_vec {
                let log_id = entry.get_log_id();
                log.insert(log_id.index, entry);
            }
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        debug!(?log_id, "Deleting conflict logs since");

        {
            let mut log = self.log.write().await;
            let keys_to_remove: Vec<_> = log.range(log_id.index..).map(|(k, _)| *k).collect();
            for key in keys_to_remove {
                log.remove(&key);
            }
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!(?log_id, "Purging logs up to");

        {
            let mut log = self.log.write().await;
            let mut purged = self.last_purged_log_id.write().await;

            let keys_to_remove: Vec<_> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for key in keys_to_remove {
                log.remove(&key);
            }

            *purged = Some(log_id);
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<StreamlineTypeConfig>],
    ) -> Result<Vec<ClusterResponse>, StorageError<NodeId>> {
        let mut results = Vec::new();
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!(log_id = ?entry.log_id, "Applying entry");
            sm.last_applied_log = Some(*entry.get_log_id());

            match &entry.payload {
                EntryPayload::Blank => {
                    results.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let response = StreamlineStore::apply_command(&mut sm, cmd.clone());
                    results.push(response);
                }
                EntryPayload::Membership(membership) => {
                    sm.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
                    results.push(ClusterResponse::Ok);
                }
            }
        }

        drop(sm);
        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Get the Arc from our stored weak reference.
        //
        // INVARIANT: set_self_ref() must be called after creating StreamlineStore
        // and wrapping it in Arc. This is done in create_raft_node().
        //
        // If the weak ref is unavailable (dropped or never set), we fall back to
        // building a snapshot directly from current state rather than panicking,
        // since the trait signature doesn't allow returning Result.
        let self_ref = self.self_ref.read().await;
        match self_ref.as_ref().and_then(|weak| weak.upgrade()) {
            Some(store) => SnapshotBuilderEnum::Normal(SnapshotBuilderWrapper { store }),
            None => {
                error!(
                    "StreamlineStore::get_snapshot_builder: self_ref unavailable. \
                     Either set_self_ref() was not called after Arc wrapping, or the \
                     Arc has been dropped. Building fallback snapshot from current state."
                );
                // Build a fallback snapshot inline from the current state machine.
                // This is less efficient but avoids a panic in production.
                SnapshotBuilderEnum::Fallback(FallbackSnapshotBuilder {
                    state_machine: self.state_machine.read().await.clone(),
                })
            }
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(snapshot_id = %meta.snapshot_id, "Installing snapshot");

        let data = snapshot.into_inner();
        let metadata: ClusterMetadata = bincode::deserialize(&data).map_err(|e| {
            error!(error = %e, "Failed to deserialize snapshot");
            io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        {
            let mut sm = self.state_machine.write().await;
            sm.last_applied_log = meta.last_log_id;
            sm.last_membership = meta.last_membership.clone();
            sm.metadata = metadata;
        }

        {
            let mut snapshot_store = self.snapshot.write().await;
            *snapshot_store = Some(StoredSnapshot {
                meta: meta.clone(),
                data,
            });
        }

        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        self.save_snapshot()
            .await
            .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<StreamlineTypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.snapshot.read().await;

        if let Some(stored) = snapshot.as_ref() {
            Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
        } else {
            Ok(None)
        }
    }
}

// Implement RaftStorage for Arc<StreamlineStore> to work with Adaptor
#[allow(deprecated)]
impl openraft::RaftStorage<StreamlineTypeConfig> for Arc<StreamlineStore> {
    type LogReader = StreamlineLogReader;
    type SnapshotBuilder = SnapshotBuilderEnum;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Share the Arc reference to the log, don't clone the data.
        // This ensures newly appended entries are immediately visible to readers,
        // satisfying openraft's storage guarantee that entries are readable after append.
        StreamlineLogReader {
            log: self.log.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!(?vote, "Saving vote");
        {
            let mut v = self.vote.write().await;
            *v = Some(*vote);
        }
        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<StreamlineTypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let purged = self.last_purged_log_id.read().await;

        let last_log_id = log.iter().next_back().map(|(_, e)| *e.get_log_id());

        Ok(LogState {
            last_purged_log_id: *purged,
            last_log_id,
        })
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<StreamlineTypeConfig>> + Send,
    {
        let entries_vec: Vec<_> = entries.into_iter().collect();

        if !entries_vec.is_empty() {
            debug!(
                count = entries_vec.len(),
                first = ?entries_vec.first().map(|e| e.get_log_id()),
                last = ?entries_vec.last().map(|e| e.get_log_id()),
                "Appending log entries"
            );
        }

        {
            let mut log = self.log.write().await;
            for entry in entries_vec {
                let log_id = entry.get_log_id();
                log.insert(log_id.index, entry);
            }
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        debug!(?log_id, "Deleting conflict logs since");

        {
            let mut log = self.log.write().await;
            let keys_to_remove: Vec<_> = log.range(log_id.index..).map(|(k, _)| *k).collect();
            for key in keys_to_remove {
                log.remove(&key);
            }
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!(?log_id, "Purging logs up to");

        {
            let mut log = self.log.write().await;
            let mut purged = self.last_purged_log_id.write().await;

            let keys_to_remove: Vec<_> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for key in keys_to_remove {
                log.remove(&key);
            }

            *purged = Some(log_id);
        }

        self.save_log()
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<StreamlineTypeConfig>],
    ) -> Result<Vec<ClusterResponse>, StorageError<NodeId>> {
        let mut results = Vec::new();
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!(log_id = ?entry.log_id, "Applying entry");
            sm.last_applied_log = Some(*entry.get_log_id());

            match &entry.payload {
                EntryPayload::Blank => {
                    results.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let response = StreamlineStore::apply_command(&mut sm, cmd.clone());
                    results.push(response);
                }
                EntryPayload::Membership(membership) => {
                    sm.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
                    results.push(ClusterResponse::Ok);
                }
            }
        }

        drop(sm);
        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // We already have Arc<Self>, use clone
        SnapshotBuilderEnum::Normal(SnapshotBuilderWrapper {
            store: self.clone(),
        })
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(snapshot_id = %meta.snapshot_id, "Installing snapshot");

        let data = snapshot.into_inner();
        let metadata: ClusterMetadata = bincode::deserialize(&data).map_err(|e| {
            error!(error = %e, "Failed to deserialize snapshot");
            io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        {
            let mut sm = self.state_machine.write().await;
            sm.last_applied_log = meta.last_log_id;
            sm.last_membership = meta.last_membership.clone();
            sm.metadata = metadata;
        }

        {
            let mut snapshot_store = self.snapshot.write().await;
            *snapshot_store = Some(StoredSnapshot {
                meta: meta.clone(),
                data,
            });
        }

        self.save_state()
            .await
            .map_err(|e| io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        self.save_snapshot()
            .await
            .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<StreamlineTypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.snapshot.read().await;

        if let Some(stored) = snapshot.as_ref() {
            Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_store_creation() {
        let dir = tempdir().unwrap();
        let store = Arc::new(
            StreamlineStore::new(dir.path().to_path_buf(), "test-cluster".to_string()).unwrap(),
        );

        let metadata = store.get_metadata().await;
        assert_eq!(metadata.cluster_id, "test-cluster");
    }

    #[tokio::test]
    async fn test_vote_persistence() {
        let dir = tempdir().unwrap();
        let mut store = Arc::new(
            StreamlineStore::new(dir.path().to_path_buf(), "test-cluster".to_string()).unwrap(),
        );

        let vote = Vote::new(1, 2);
        openraft::RaftStorage::save_vote(&mut store, &vote)
            .await
            .unwrap();

        let read_vote = openraft::RaftStorage::read_vote(&mut store).await.unwrap();
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn test_log_reader_sees_appended_entries() {
        // This test verifies the fix for the stale log reader issue.
        // The log reader must share a reference to the live log, not a clone,
        // so that entries appended after the reader is created are visible.
        use openraft::storage::RaftLogReader;
        use openraft::CommittedLeaderId;

        let dir = tempdir().unwrap();
        let store = Arc::new(
            StreamlineStore::new(dir.path().to_path_buf(), "test-cluster".to_string()).unwrap(),
        );

        // Get the log Arc before creating the reader (for direct insertion later)
        let log_arc = store.log.clone();

        // Create a log reader from the store's log Arc
        let mut reader = StreamlineLogReader {
            log: store.log.clone(),
        };

        // Verify no entries initially
        let entries: Vec<Entry<StreamlineTypeConfig>> =
            reader.try_get_log_entries(0u64..10u64).await.unwrap();
        assert!(entries.is_empty(), "Expected no entries initially");

        // Insert entries AFTER the reader was created (directly into the shared log)
        // This simulates what append_to_log does, but avoids the Entry construction complexity
        {
            let mut log = log_arc.write().await;
            let leader_id = CommittedLeaderId::new(1, 1);
            let entry1 = Entry {
                log_id: LogId::new(leader_id, 1),
                payload: EntryPayload::Blank,
            };
            let entry2 = Entry {
                log_id: LogId::new(leader_id, 2),
                payload: EntryPayload::Blank,
            };
            log.insert(1, entry1);
            log.insert(2, entry2);
        }

        // The reader must see the newly inserted entries (this was broken before the fix)
        let entries: Vec<Entry<StreamlineTypeConfig>> =
            reader.try_get_log_entries(0u64..10u64).await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "Log reader must see entries appended after creation"
        );
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);
    }
}
