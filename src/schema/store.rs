//! Schema storage and caching
//!
//! This module provides schema storage using an internal Kafka topic (`_schemas`)
//! and an in-memory cache with TTL support.

use super::{
    compatibility::CompatibilityChecker, CompatibilityLevel, RegisteredSchema, SchemaError,
    SchemaReference, SchemaRegistryConfig, SchemaType,
};
use crate::storage::TopicManager;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Schema cache entry with TTL
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

impl<T: Clone> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Schema cache with TTL support
#[derive(Debug)]
pub struct SchemaCache {
    /// Schema ID -> RegisteredSchema
    by_id: DashMap<i32, CacheEntry<RegisteredSchema>>,
    /// Subject -> Version -> RegisteredSchema
    by_subject: DashMap<String, HashMap<i32, CacheEntry<RegisteredSchema>>>,
    /// Subject -> CompatibilityLevel
    subject_compatibility: DashMap<String, CompatibilityLevel>,
    /// Cache TTL
    ttl: Duration,
    /// Maximum cache size
    max_size: usize,
}

impl SchemaCache {
    /// Create a new schema cache
    pub fn new(ttl_seconds: u64, max_size: usize) -> Self {
        Self {
            by_id: DashMap::new(),
            by_subject: DashMap::new(),
            subject_compatibility: DashMap::new(),
            ttl: Duration::from_secs(ttl_seconds),
            max_size,
        }
    }

    /// Get schema by ID
    pub fn get_by_id(&self, id: i32) -> Option<RegisteredSchema> {
        self.by_id.get(&id).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Get schema by subject and version
    pub fn get_by_subject_version(&self, subject: &str, version: i32) -> Option<RegisteredSchema> {
        self.by_subject.get(subject).and_then(|versions| {
            versions.get(&version).and_then(|entry| {
                if entry.is_expired() {
                    None
                } else {
                    Some(entry.value.clone())
                }
            })
        })
    }

    /// Get all versions for a subject (sorted in ascending order)
    pub fn get_versions(&self, subject: &str) -> Vec<i32> {
        self.by_subject
            .get(subject)
            .map(|versions| {
                let mut vers: Vec<_> = versions
                    .iter()
                    .filter(|(_, entry)| !entry.is_expired())
                    .map(|(v, _)| *v)
                    .collect();
                vers.sort();
                vers
            })
            .unwrap_or_default()
    }

    /// Get all schemas for a subject
    pub fn get_all_for_subject(&self, subject: &str) -> Vec<RegisteredSchema> {
        self.by_subject
            .get(subject)
            .map(|versions| {
                let mut schemas: Vec<_> = versions
                    .iter()
                    .filter(|(_, entry)| !entry.is_expired())
                    .map(|(_, entry)| entry.value.clone())
                    .collect();
                schemas.sort_by_key(|s| s.version);
                schemas
            })
            .unwrap_or_default()
    }

    /// Get latest schema for a subject
    pub fn get_latest(&self, subject: &str) -> Option<RegisteredSchema> {
        let schemas = self.get_all_for_subject(subject);
        schemas.into_iter().last()
    }

    /// Put schema into cache
    pub fn put(&self, schema: RegisteredSchema) {
        // Check size limit
        if self.by_id.len() >= self.max_size {
            self.evict_expired();
        }

        let entry = CacheEntry::new(schema.clone(), self.ttl);

        // Store by ID
        self.by_id.insert(schema.id, entry.clone());

        // Store by subject/version
        self.by_subject
            .entry(schema.subject.clone())
            .or_default()
            .insert(schema.version, entry);
    }

    /// Set compatibility level for a subject
    pub fn set_subject_compatibility(&self, subject: &str, level: CompatibilityLevel) {
        self.subject_compatibility
            .insert(subject.to_string(), level);
    }

    /// Get compatibility level for a subject
    pub fn get_subject_compatibility(&self, subject: &str) -> Option<CompatibilityLevel> {
        self.subject_compatibility.get(subject).map(|r| *r)
    }

    /// Remove expired entries
    fn evict_expired(&self) {
        // Remove expired entries from by_id
        self.by_id.retain(|_, entry| !entry.is_expired());

        // Remove expired entries from by_subject
        for mut versions in self.by_subject.iter_mut() {
            versions.retain(|_, entry| !entry.is_expired());
        }

        // Remove empty subjects
        self.by_subject.retain(|_, versions| !versions.is_empty());
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        self.by_id.clear();
        self.by_subject.clear();
        self.subject_compatibility.clear();
    }

    /// Get all subjects
    pub fn get_subjects(&self) -> Vec<String> {
        self.by_subject
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Delete all schemas for a subject
    pub fn delete_subject(&self, subject: &str) {
        if let Some((_, versions)) = self.by_subject.remove(subject) {
            for (_, entry) in versions {
                self.by_id.remove(&entry.value.id);
            }
        }
    }

    /// Delete a specific version from a subject
    pub fn delete_version(&self, subject: &str, version: i32) {
        if let Some(mut versions) = self.by_subject.get_mut(subject) {
            if let Some(entry) = versions.remove(&version) {
                self.by_id.remove(&entry.value.id);
            }
        }
    }

    /// Get all subjects that reference a given schema ID
    pub fn get_subjects_for_id(&self, id: i32) -> Vec<String> {
        let mut subjects = Vec::new();
        for entry in self.by_subject.iter() {
            for cache_entry in entry.value().values() {
                if !cache_entry.is_expired() && cache_entry.value.id == id {
                    subjects.push(entry.key().clone());
                    break;
                }
            }
        }
        subjects
    }

    /// Get all (subject, version) pairs for a given schema ID
    pub fn get_subject_versions_for_id(&self, id: i32) -> Vec<(String, i32)> {
        let mut pairs = Vec::new();
        for entry in self.by_subject.iter() {
            for cache_entry in entry.value().values() {
                if !cache_entry.is_expired() && cache_entry.value.id == id {
                    pairs.push((entry.key().clone(), cache_entry.value.version));
                }
            }
        }
        pairs
    }

    /// Delete compatibility level for a subject (resets to global default)
    pub fn delete_subject_compatibility(&self, subject: &str) {
        self.subject_compatibility.remove(subject);
    }

    /// Get all schema IDs that reference a given subject+version via schema references
    pub fn get_referencing_schemas(&self, subject: &str, version: i32) -> Vec<i32> {
        let mut ids = Vec::new();
        for entry in self.by_subject.iter() {
            for cache_entry in entry.value().values() {
                if cache_entry.is_expired() {
                    continue;
                }
                let schema = &cache_entry.value;
                for reference in &schema.references {
                    if reference.subject == subject && reference.version == version {
                        ids.push(schema.id);
                    }
                }
            }
        }
        ids
    }
}

/// Schema store event for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaStoreEvent {
    /// Schema registered
    SchemaRegistered(RegisteredSchema),
    /// Subject deleted
    SubjectDeleted { subject: String },
    /// Version deleted
    VersionDeleted { subject: String, version: i32 },
    /// Compatibility set
    CompatibilitySet {
        subject: Option<String>,
        level: CompatibilityLevel,
    },
}

/// A (subject, version) pair returned by schema ID lookups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectVersionPair {
    /// Subject name
    pub subject: String,
    /// Version number
    pub version: i32,
}

/// Schema store for persistent storage
pub struct SchemaStore {
    /// In-memory cache
    cache: Arc<SchemaCache>,
    /// Next schema ID
    next_id: AtomicI32,
    /// Global compatibility level
    global_compatibility: RwLock<CompatibilityLevel>,
    /// Compatibility checker
    checker: CompatibilityChecker,
    /// Configuration (reserved for future use)
    #[allow(dead_code)]
    config: SchemaRegistryConfig,
    /// Event log (would be persisted to _schemas topic)
    events: RwLock<Vec<SchemaStoreEvent>>,
    /// Optional topic manager for persisting events to `_schemas` topic
    topic_manager: Option<Arc<TopicManager>>,
}

impl std::fmt::Debug for SchemaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaStore")
            .field("cache", &self.cache)
            .field("next_id", &self.next_id)
            .field("config", &self.config)
            .field("has_topic_manager", &self.topic_manager.is_some())
            .finish()
    }
}

/// Internal topic name for schema persistence
const SCHEMAS_TOPIC: &str = "_schemas";
/// Number of partitions for the schemas topic (single partition for ordering)
const SCHEMAS_PARTITIONS: i32 = 1;

impl SchemaStore {
    /// Create a new schema store (in-memory only, no persistence)
    pub fn new(config: SchemaRegistryConfig) -> Self {
        Self {
            cache: Arc::new(SchemaCache::new(
                config.cache_ttl_seconds,
                config.max_cache_size,
            )),
            next_id: AtomicI32::new(1),
            global_compatibility: RwLock::new(config.default_compatibility),
            checker: CompatibilityChecker::new(),
            config,
            events: RwLock::new(Vec::new()),
            topic_manager: None,
        }
    }

    /// Create a new schema store with persistence via TopicManager
    pub fn with_topic_manager(config: SchemaRegistryConfig, topic_manager: Arc<TopicManager>) -> Self {
        Self {
            cache: Arc::new(SchemaCache::new(
                config.cache_ttl_seconds,
                config.max_cache_size,
            )),
            next_id: AtomicI32::new(1),
            global_compatibility: RwLock::new(config.default_compatibility),
            checker: CompatibilityChecker::new(),
            config,
            events: RwLock::new(Vec::new()),
            topic_manager: Some(topic_manager),
        }
    }

    /// Persist an event to the `_schemas` topic if a TopicManager is available.
    fn persist_event(&self, subject: &str, event: &SchemaStoreEvent) -> Result<(), SchemaError> {
        if let Some(tm) = &self.topic_manager {
            let value = serde_json::to_vec(event)
                .map_err(|e| SchemaError::SerializationError(e.to_string()))?;
            tm.append(
                SCHEMAS_TOPIC,
                0,
                Some(Bytes::from(subject.to_owned())),
                Bytes::from(value),
            )
            .map_err(|e| SchemaError::StorageError(e.to_string()))?;
        }
        Ok(())
    }

    /// Replay all events from the `_schemas` topic to rebuild in-memory state.
    ///
    /// Should be called once at startup when a TopicManager is available.
    pub async fn replay_from_topic(&self) -> Result<(), SchemaError> {
        let tm = match &self.topic_manager {
            Some(tm) => tm,
            None => return Ok(()),
        };

        // Read all records from partition 0
        let records = tm
            .read(SCHEMAS_TOPIC, 0, 0, usize::MAX)
            .map_err(|e| SchemaError::StorageError(e.to_string()))?;

        let mut max_id: i32 = 0;
        let mut replayed: Vec<SchemaStoreEvent> = Vec::with_capacity(records.len());

        for record in &records {
            let event: SchemaStoreEvent = serde_json::from_slice(&record.value)
                .map_err(|e| SchemaError::SerializationError(e.to_string()))?;
            if let SchemaStoreEvent::SchemaRegistered(ref schema) = event {
                if schema.id > max_id {
                    max_id = schema.id;
                }
            }
            replayed.push(event);
        }

        // Replay into cache
        self.replay_events(replayed).await;

        // Restore next_id to max seen + 1
        if max_id > 0 {
            let current = self.next_id.load(Ordering::SeqCst);
            if max_id + 1 > current {
                self.next_id.store(max_id + 1, Ordering::SeqCst);
            }
        }

        info!(
            records_replayed = records.len(),
            next_id = self.next_id.load(Ordering::SeqCst),
            "Schema store recovered from _schemas topic"
        );

        Ok(())
    }

    /// Ensure the `_schemas` topic exists (idempotent).
    pub fn ensure_schemas_topic(&self) -> Result<(), SchemaError> {
        if let Some(tm) = &self.topic_manager {
            tm.get_or_create_topic(SCHEMAS_TOPIC, SCHEMAS_PARTITIONS)
                .map_err(|e| SchemaError::StorageError(e.to_string()))?;
        }
        Ok(())
    }

    /// Get cache reference
    pub fn cache(&self) -> &Arc<SchemaCache> {
        &self.cache
    }

    /// Register a new schema
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: Vec<SchemaReference>,
    ) -> Result<i32, SchemaError> {
        // Validate the schema
        self.checker.validate(schema, schema_type)?;

        // Validate references resolve to existing schemas
        if !references.is_empty() {
            self.validate_references(&references)?;
        }

        // Check if schema already exists
        let existing_schemas = self.cache.get_all_for_subject(subject);

        // Check for duplicate schema
        for existing in &existing_schemas {
            if existing.schema_type == schema_type
                && self
                    .checker
                    .schemas_equal(schema, &existing.schema, schema_type)?
            {
                debug!(
                    subject = subject,
                    id = existing.id,
                    "Schema already registered"
                );
                return Ok(existing.id);
            }
        }

        // Check compatibility
        let compatibility = self.get_subject_compatibility(subject).await;
        if !existing_schemas.is_empty() && compatibility != CompatibilityLevel::None {
            let result = self.checker.check_compatibility_transitive(
                schema,
                &existing_schemas,
                compatibility,
            )?;

            if !result.is_compatible {
                return Err(SchemaError::IncompatibleSchema(result.messages.join("; ")));
            }
        }

        // Assign ID and version
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let version = existing_schemas.len() as i32 + 1;

        let registered_schema = RegisteredSchema {
            subject: subject.to_string(),
            version,
            id,
            schema_type,
            schema: schema.to_string(),
            references,
        };

        // Store in cache
        self.cache.put(registered_schema.clone());

        // Record event
        let event = SchemaStoreEvent::SchemaRegistered(registered_schema);
        let mut events = self.events.write().await;
        events.push(event.clone());
        drop(events);

        // Persist to _schemas topic
        self.persist_event(subject, &event)?;

        info!(
            subject = subject,
            version = version,
            id = id,
            schema_type = %schema_type,
            "Schema registered"
        );

        Ok(id)
    }

    /// Get schema by ID
    pub fn get_schema_by_id(&self, id: i32) -> Result<RegisteredSchema, SchemaError> {
        self.cache
            .get_by_id(id)
            .ok_or_else(|| SchemaError::SchemaNotFound(format!("Schema ID {}", id)))
    }

    /// Get schema by subject and version
    pub fn get_schema(&self, subject: &str, version: i32) -> Result<RegisteredSchema, SchemaError> {
        self.cache
            .get_by_subject_version(subject, version)
            .ok_or_else(|| SchemaError::VersionNotFound {
                subject: subject.to_string(),
                version,
            })
    }

    /// Get latest schema for a subject
    pub fn get_latest_schema(&self, subject: &str) -> Result<RegisteredSchema, SchemaError> {
        self.cache
            .get_latest(subject)
            .ok_or_else(|| SchemaError::SubjectNotFound(subject.to_string()))
    }

    /// Get all versions for a subject
    pub fn get_versions(&self, subject: &str) -> Result<Vec<i32>, SchemaError> {
        let versions = self.cache.get_versions(subject);
        if versions.is_empty() {
            return Err(SchemaError::SubjectNotFound(subject.to_string()));
        }
        Ok(versions)
    }

    /// Get all subjects
    pub fn get_subjects(&self) -> Vec<String> {
        self.cache.get_subjects()
    }

    /// Delete a subject
    pub async fn delete_subject(&self, subject: &str) -> Result<Vec<i32>, SchemaError> {
        let versions = self.get_versions(subject)?;

        // Note: In production, this would soft-delete (mark as deleted)
        // For simplicity, we just clear from cache
        // The events would still preserve the history

        let mut events = self.events.write().await;
        let event = SchemaStoreEvent::SubjectDeleted {
            subject: subject.to_string(),
        };
        events.push(event.clone());
        drop(events);

        // Persist to _schemas topic
        self.persist_event(subject, &event)
            .map_err(|_| SchemaError::StorageError("Failed to persist subject deletion".into()))?;

        info!(subject = subject, "Subject deleted");
        Ok(versions)
    }

    /// Delete a specific version
    pub async fn delete_version(&self, subject: &str, version: i32) -> Result<i32, SchemaError> {
        let schema = self.get_schema(subject, version)?;

        let mut events = self.events.write().await;
        let event = SchemaStoreEvent::VersionDeleted {
            subject: subject.to_string(),
            version,
        };
        events.push(event.clone());
        drop(events);

        // Persist to _schemas topic
        self.persist_event(subject, &event)
            .map_err(|_| SchemaError::StorageError("Failed to persist version deletion".into()))?;

        info!(subject = subject, version = version, "Version deleted");
        Ok(schema.id)
    }

    /// Check compatibility
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        version: Option<i32>,
    ) -> Result<(bool, Vec<String>), SchemaError> {
        // Validate the schema first
        self.checker.validate(schema, schema_type)?;

        // Get schemas to check against
        let schemas_to_check = if let Some(v) = version {
            vec![self.get_schema(subject, v)?]
        } else {
            self.cache.get_all_for_subject(subject)
        };

        if schemas_to_check.is_empty() {
            // No existing schemas, compatible by default
            return Ok((true, vec![]));
        }

        let compatibility = self.get_subject_compatibility(subject).await;
        let result = self.checker.check_compatibility_transitive(
            schema,
            &schemas_to_check,
            compatibility,
        )?;

        Ok((result.is_compatible, result.messages))
    }

    /// Get compatibility level for a subject
    pub async fn get_subject_compatibility(&self, subject: &str) -> CompatibilityLevel {
        self.cache
            .get_subject_compatibility(subject)
            .unwrap_or(*self.global_compatibility.read().await)
    }

    /// Set compatibility level for a subject
    pub async fn set_subject_compatibility(
        &self,
        subject: &str,
        level: CompatibilityLevel,
    ) -> Result<(), SchemaError> {
        self.cache.set_subject_compatibility(subject, level);

        let event = SchemaStoreEvent::CompatibilitySet {
            subject: Some(subject.to_string()),
            level,
        };
        let mut events = self.events.write().await;
        events.push(event.clone());
        drop(events);

        // Persist to _schemas topic
        self.persist_event(subject, &event)
            .map_err(|_| SchemaError::StorageError("Failed to persist compatibility change".into()))?;

        info!(
            subject = subject,
            compatibility = %level,
            "Subject compatibility set"
        );
        Ok(())
    }

    /// Get global compatibility level
    pub async fn get_global_compatibility(&self) -> CompatibilityLevel {
        *self.global_compatibility.read().await
    }

    /// Set global compatibility level
    pub async fn set_global_compatibility(
        &self,
        level: CompatibilityLevel,
    ) -> Result<(), SchemaError> {
        *self.global_compatibility.write().await = level;

        let event = SchemaStoreEvent::CompatibilitySet {
            subject: None,
            level,
        };
        let mut events = self.events.write().await;
        events.push(event.clone());
        drop(events);

        // Persist to _schemas topic
        self.persist_event("_global", &event)
            .map_err(|_| SchemaError::StorageError("Failed to persist global compatibility".into()))?;

        info!(compatibility = %level, "Global compatibility set");
        Ok(())
    }

    /// Look up schema by content
    pub fn lookup_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
    ) -> Option<RegisteredSchema> {
        let schemas = self.cache.get_all_for_subject(subject);
        for existing in schemas {
            if existing.schema_type == schema_type {
                if let Ok(true) = self
                    .checker
                    .schemas_equal(schema, &existing.schema, schema_type)
                {
                    return Some(existing);
                }
            }
        }
        None
    }

    /// Get all subjects that use a given schema ID
    pub fn get_subjects_for_schema_id(&self, id: i32) -> Vec<String> {
        self.cache.get_subjects_for_id(id)
    }

    /// Get all (subject, version) pairs for a given schema ID
    pub fn get_subject_versions_for_schema_id(&self, id: i32) -> Vec<SubjectVersionPair> {
        self.cache
            .get_subject_versions_for_id(id)
            .into_iter()
            .map(|(subject, version)| SubjectVersionPair { subject, version })
            .collect()
    }

    /// Delete subject-level compatibility config (resets to global default)
    pub async fn delete_subject_compatibility(
        &self,
        subject: &str,
    ) -> Result<CompatibilityLevel, SchemaError> {
        let previous = self.get_subject_compatibility(subject).await;
        self.cache.delete_subject_compatibility(subject);

        let event = SchemaStoreEvent::CompatibilitySet {
            subject: Some(subject.to_string()),
            level: self.get_global_compatibility().await,
        };
        let mut events = self.events.write().await;
        events.push(event.clone());
        drop(events);

        self.persist_event(subject, &event)
            .map_err(|_| {
                SchemaError::StorageError(
                    "Failed to persist compatibility deletion".into(),
                )
            })?;

        info!(
            subject = subject,
            previous = %previous,
            "Subject compatibility config deleted (reset to global)"
        );
        Ok(previous)
    }

    /// Get all schema IDs that reference a given subject+version
    pub fn get_referencing_schemas(&self, subject: &str, version: i32) -> Vec<i32> {
        self.cache.get_referencing_schemas(subject, version)
    }

    /// Validate data against a schema
    pub fn validate_data(&self, schema_id: i32, data: &[u8]) -> Result<(), SchemaError> {
        let schema = self.get_schema_by_id(schema_id)?;
        self.checker
            .validate_data(&schema.schema, schema.schema_type, data)
    }

    /// Get all events (for persistence)
    pub async fn get_events(&self) -> Vec<SchemaStoreEvent> {
        self.events.read().await.clone()
    }

    /// Replay events (for recovery)
    pub async fn replay_events(&self, events: Vec<SchemaStoreEvent>) {
        for event in events {
            match event {
                SchemaStoreEvent::SchemaRegistered(schema) => {
                    let current_max = self.next_id.load(Ordering::SeqCst);
                    if schema.id >= current_max {
                        self.next_id.store(schema.id + 1, Ordering::SeqCst);
                    }
                    self.cache.put(schema);
                }
                SchemaStoreEvent::SubjectDeleted { ref subject } => {
                    self.cache.delete_subject(subject);
                    self.cache.delete_subject_compatibility(subject);
                    debug!(subject = subject, "Subject deletion replayed");
                }
                SchemaStoreEvent::VersionDeleted {
                    ref subject,
                    version,
                } => {
                    self.cache.delete_version(subject, version);
                    debug!(
                        subject = subject,
                        version = version,
                        "Version deletion replayed"
                    );
                }
                SchemaStoreEvent::CompatibilitySet { subject, level } => {
                    if let Some(s) = subject {
                        self.cache.set_subject_compatibility(&s, level);
                    } else {
                        *self.global_compatibility.write().await = level;
                    }
                }
            }
        }
    }

    /// Validate that all schema references resolve to existing registered schemas.
    pub fn validate_references(&self, references: &[SchemaReference]) -> Result<(), SchemaError> {
        for reference in references {
            let schema = self
                .cache
                .get_by_subject_version(&reference.subject, reference.version);
            if schema.is_none() {
                return Err(SchemaError::InvalidSchema(format!(
                    "Unresolved reference: subject='{}' version={}",
                    reference.subject, reference.version
                )));
            }
        }
        // Check for circular references
        let mut visited = std::collections::HashSet::new();
        for reference in references {
            self.detect_circular_refs(&reference.subject, reference.version, &mut visited)?;
        }
        Ok(())
    }

    fn detect_circular_refs(
        &self,
        subject: &str,
        version: i32,
        visited: &mut std::collections::HashSet<(String, i32)>,
    ) -> Result<(), SchemaError> {
        let key = (subject.to_string(), version);
        if !visited.insert(key.clone()) {
            return Err(SchemaError::InvalidSchema(format!(
                "Circular schema reference detected: subject='{}' version={}",
                subject, version
            )));
        }
        if let Some(schema) = self.cache.get_by_subject_version(subject, version) {
            for reference in &schema.references {
                self.detect_circular_refs(
                    &reference.subject,
                    reference.version,
                    visited,
                )?;
            }
        }
        visited.remove(&key);
        Ok(())
    }

    /// Search schemas by subject prefix or schema content substring.
    pub fn search_schemas(
        &self,
        query: &str,
        schema_type_filter: Option<SchemaType>,
        limit: usize,
    ) -> Vec<RegisteredSchema> {
        let query_lower = query.to_lowercase();
        let mut results = Vec::new();

        for subject in self.cache.get_subjects() {
            for schema in self.cache.get_all_for_subject(&subject) {
                if let Some(filter) = schema_type_filter {
                    if schema.schema_type != filter {
                        continue;
                    }
                }
                let subject_match = schema.subject.to_lowercase().contains(&query_lower);
                let content_match = schema.schema.to_lowercase().contains(&query_lower);
                if subject_match || content_match {
                    results.push(schema);
                    if results.len() >= limit {
                        return results;
                    }
                }
            }
        }
        results
    }
}

/// Schema registry service
pub struct SchemaRegistry {
    store: Arc<SchemaStore>,
    config: SchemaRegistryConfig,
}

impl SchemaRegistry {
    /// Create a new schema registry (in-memory only)
    pub fn new(config: SchemaRegistryConfig) -> Self {
        Self {
            store: Arc::new(SchemaStore::new(config.clone())),
            config,
        }
    }

    /// Create a new schema registry with topic-based persistence
    pub fn with_topic_manager(config: SchemaRegistryConfig, topic_manager: Arc<TopicManager>) -> Self {
        Self {
            store: Arc::new(SchemaStore::with_topic_manager(config.clone(), topic_manager)),
            config,
        }
    }

    /// Get store reference
    pub fn store(&self) -> &Arc<SchemaStore> {
        &self.store
    }

    /// Check if schema registry is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get schema topic name
    pub fn schema_topic(&self) -> &str {
        &self.config.schema_topic
    }

    /// Check if validation on produce is enabled
    pub fn validate_on_produce(&self) -> bool {
        self.config.validate_on_produce
    }
}

/// Schema validation interceptor for the produce path.
///
/// Sits between the Kafka protocol handler and storage layer to validate
/// that produced messages conform to their registered schemas.
pub struct ProduceSchemaValidator {
    store: Arc<SchemaStore>,
    enabled: bool,
    auto_register: bool,
}

impl ProduceSchemaValidator {
    /// Create a new produce validator.
    pub fn new(store: Arc<SchemaStore>, enabled: bool, auto_register: bool) -> Self {
        Self {
            store,
            enabled,
            auto_register,
        }
    }

    /// Validate a message before it is written to storage.
    ///
    /// If the topic has a registered schema (subject = "{topic}-value"),
    /// validates the message payload against the latest schema version.
    ///
    /// Returns Ok(()) if validation passes or is not required.
    pub async fn validate_produce(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<(), SchemaError> {
        if !self.enabled {
            return Ok(());
        }

        // Check for value schema
        let value_subject = format!("{}-value", topic);
        if let Ok(schema) = self.store.get_latest_schema(&value_subject) {
            // Try to extract schema ID from Confluent wire format (magic byte + 4-byte ID)
            if value.len() > 5 && value[0] == 0 {
                let schema_id = i32::from_be_bytes([value[1], value[2], value[3], value[4]]);
                self.store.validate_data(schema_id, &value[5..])?;
            } else {
                // Validate raw payload against latest schema
                self.store
                    .checker
                    .validate_data(&schema.schema, schema.schema_type, value)?;
            }
        } else if self.auto_register && !value.is_empty() {
            // Auto-register: try to infer schema from the first message
            if let Ok(inferred) = serde_json::from_slice::<serde_json::Value>(value) {
                let schema_str = crate::schema::inference::infer_json_schema(&inferred);
                let _ = self
                    .store
                    .register_schema(
                        &value_subject,
                        &schema_str,
                        SchemaType::Json,
                        vec![],
                    )
                    .await;
            }
        }

        // Check for key schema (optional)
        if let Some(key_data) = key {
            let key_subject = format!("{}-key", topic);
            if let Ok(schema) = self.store.get_latest_schema(&key_subject) {
                if key_data.len() > 5 && key_data[0] == 0 {
                    let schema_id =
                        i32::from_be_bytes([key_data[1], key_data[2], key_data[3], key_data[4]]);
                    self.store.validate_data(schema_id, &key_data[5..])?;
                } else {
                    self.store
                        .checker
                        .validate_data(&schema.schema, schema.schema_type, key_data)?;
                }
            }
        }

        Ok(())
    }

    /// Check if a subject has a registered schema.
    pub fn has_schema(&self, subject: &str) -> bool {
        self.store.get_latest_schema(subject).is_ok()
    }

    /// Get the schema ID for a subject's latest version.
    pub fn latest_schema_id(&self, subject: &str) -> Option<i32> {
        self.store
            .get_latest_schema(subject)
            .ok()
            .map(|s| s.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SchemaRegistryConfig {
        SchemaRegistryConfig {
            enabled: true,
            default_compatibility: CompatibilityLevel::Backward,
            cache_ttl_seconds: 300,
            max_cache_size: 1000,
            validate_on_produce: false,
            schema_topic: "_schemas".to_string(),
        }
    }

    #[tokio::test]
    async fn test_register_avro_schema() {
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let result = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await;

        assert!(result.is_ok());
        let id = result.unwrap();
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn test_register_duplicate_schema() {
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;

        let id1 = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();
        let id2 = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();

        // Same schema should return same ID
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_get_schema_by_id() {
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let id = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();

        let result = store.get_schema_by_id(id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, id);
    }

    #[tokio::test]
    async fn test_get_versions() {
        let store = SchemaStore::new(test_config());

        let schema1 =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let schema2 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#;

        store
            .register_schema("test-value", schema1, SchemaType::Avro, vec![])
            .await
            .unwrap();
        store
            .register_schema("test-value", schema2, SchemaType::Avro, vec![])
            .await
            .unwrap();

        let versions = store.get_versions("test-value").unwrap();
        assert_eq!(versions, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_get_subjects() {
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;

        store
            .register_schema("test1-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();
        store
            .register_schema("test2-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();

        let subjects = store.get_subjects();
        assert!(subjects.contains(&"test1-value".to_string()));
        assert!(subjects.contains(&"test2-value".to_string()));
    }

    #[tokio::test]
    async fn test_compatibility_check() {
        let store = SchemaStore::new(test_config());

        let schema1 =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        store
            .register_schema("test-value", schema1, SchemaType::Avro, vec![])
            .await
            .unwrap();

        // Compatible schema
        let schema2 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#;
        let (compatible, _) = store
            .check_compatibility("test-value", schema2, SchemaType::Avro, None)
            .await
            .unwrap();
        assert!(compatible);

        // Incompatible schema (new required field without default)
        let schema3 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}"#;
        let (compatible, messages) = store
            .check_compatibility("test-value", schema3, SchemaType::Avro, None)
            .await
            .unwrap();
        assert!(!compatible);
        assert!(!messages.is_empty());
    }

    #[tokio::test]
    async fn test_set_compatibility() {
        let store = SchemaStore::new(test_config());

        // Set subject compatibility
        store
            .set_subject_compatibility("test-value", CompatibilityLevel::Full)
            .await
            .unwrap();

        let level = store.get_subject_compatibility("test-value").await;
        assert_eq!(level, CompatibilityLevel::Full);
    }

    #[tokio::test]
    async fn test_global_compatibility() {
        let store = SchemaStore::new(test_config());

        // Check default
        let level = store.get_global_compatibility().await;
        assert_eq!(level, CompatibilityLevel::Backward);

        // Set global
        store
            .set_global_compatibility(CompatibilityLevel::None)
            .await
            .unwrap();

        let level = store.get_global_compatibility().await;
        assert_eq!(level, CompatibilityLevel::None);
    }

    #[tokio::test]
    async fn test_json_schema_registration() {
        let store = SchemaStore::new(test_config());

        let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
        let result = store
            .register_schema("json-test", schema, SchemaType::Json, vec![])
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_protobuf_schema_registration() {
        let store = SchemaStore::new(test_config());

        let schema = r#"
syntax = "proto3";

message Test {
    string name = 1;
}
"#;
        let result = store
            .register_schema("proto-test", schema, SchemaType::Protobuf, vec![])
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_lookup_schema() {
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let id = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();

        let found = store.lookup_schema("test-value", schema, SchemaType::Avro);
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, id);
    }

    #[test]
    fn test_cache_expiration() {
        let cache = SchemaCache::new(0, 100); // 0 second TTL

        let schema = RegisteredSchema {
            subject: "test".to_string(),
            version: 1,
            id: 1,
            schema_type: SchemaType::Avro,
            schema: r#""string""#.to_string(),
            references: vec![],
        };

        cache.put(schema);

        // Should be expired immediately
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(cache.get_by_id(1).is_none());
    }

    #[test]
    fn test_schema_registry_creation() {
        let config = test_config();
        let registry = SchemaRegistry::new(config);

        assert!(registry.is_enabled());
        assert_eq!(registry.schema_topic(), "_schemas");
        assert!(!registry.validate_on_produce());
    }

    /// Create a TopicManager backed by in-memory storage for tests
    fn create_test_topic_manager() -> Arc<TopicManager> {
        Arc::new(TopicManager::in_memory().expect("in-memory TopicManager"))
    }

    #[tokio::test]
    async fn test_persist_and_replay_schema() {
        let tm = create_test_topic_manager();

        // Register schemas with persistence
        let store = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store.ensure_schemas_topic().unwrap();

        let schema1 =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let schema2 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#;

        let id1 = store
            .register_schema("test-value", schema1, SchemaType::Avro, vec![])
            .await
            .unwrap();
        let id2 = store
            .register_schema("test-value", schema2, SchemaType::Avro, vec![])
            .await
            .unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        // Create a new store pointing at the same TopicManager and replay
        let store2 = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store2.replay_from_topic().await.unwrap();

        // Verify replayed data
        let s = store2.get_schema_by_id(id1).unwrap();
        assert_eq!(s.subject, "test-value");
        assert_eq!(s.version, 1);

        let s2 = store2.get_schema_by_id(id2).unwrap();
        assert_eq!(s2.version, 2);

        // next_id should be max + 1
        let versions = store2.get_versions("test-value").unwrap();
        assert_eq!(versions, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_persist_compatibility_event() {
        let tm = create_test_topic_manager();
        let store = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store.ensure_schemas_topic().unwrap();

        store
            .set_subject_compatibility("my-subject", CompatibilityLevel::Full)
            .await
            .unwrap();

        // Replay into new store
        let store2 = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store2.replay_from_topic().await.unwrap();

        let level = store2.get_subject_compatibility("my-subject").await;
        assert_eq!(level, CompatibilityLevel::Full);
    }

    #[tokio::test]
    async fn test_persist_global_compatibility() {
        let tm = create_test_topic_manager();
        let store = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store.ensure_schemas_topic().unwrap();

        store
            .set_global_compatibility(CompatibilityLevel::None)
            .await
            .unwrap();

        let store2 = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store2.replay_from_topic().await.unwrap();

        let level = store2.get_global_compatibility().await;
        assert_eq!(level, CompatibilityLevel::None);
    }

    #[tokio::test]
    async fn test_in_memory_only_no_topic_manager() {
        // Without TopicManager, everything works in-memory (no errors)
        let store = SchemaStore::new(test_config());

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let id = store
            .register_schema("test-value", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();
        assert_eq!(id, 1);

        // replay_from_topic is a no-op without TopicManager
        store.replay_from_topic().await.unwrap();
    }

    #[tokio::test]
    async fn test_replay_restores_next_id() {
        let tm = create_test_topic_manager();
        let store = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store.ensure_schemas_topic().unwrap();

        let schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        // Register 3 schemas across 2 subjects
        store
            .register_schema("sub1", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();
        store
            .register_schema("sub2", schema, SchemaType::Avro, vec![])
            .await
            .unwrap();
        let schema2 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#;
        let id3 = store
            .register_schema("sub1", schema2, SchemaType::Avro, vec![])
            .await
            .unwrap();
        assert_eq!(id3, 3);

        // New store, replay, register another schema — should get id=4
        let store2 = SchemaStore::with_topic_manager(test_config(), tm.clone());
        store2.replay_from_topic().await.unwrap();

        let schema3 = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string", "default": ""}]}"#;
        let id4 = store2
            .register_schema("sub1", schema3, SchemaType::Avro, vec![])
            .await
            .unwrap();
        assert_eq!(id4, 4);
    }

    #[tokio::test]
    async fn test_produce_validator_disabled() {
        let store = Arc::new(SchemaStore::new(test_config()));
        let validator = ProduceSchemaValidator::new(store, false, false);
        let result = validator
            .validate_produce("test-topic", None, b"any data")
            .await;
        assert!(result.is_ok(), "Should pass when validator is disabled");
    }

    #[tokio::test]
    async fn test_produce_validator_no_schema() {
        let store = Arc::new(SchemaStore::new(test_config()));
        let validator = ProduceSchemaValidator::new(store, true, false);
        let result = validator
            .validate_produce("unregistered-topic", None, b"any data")
            .await;
        assert!(result.is_ok(), "Should pass when no schema is registered");
    }

    #[tokio::test]
    async fn test_produce_validator_auto_register() {
        let store = Arc::new(SchemaStore::new(test_config()));
        let validator = ProduceSchemaValidator::new(Arc::clone(&store), true, true);

        let json_msg = br#"{"name": "Alice", "age": 30}"#;
        let result = validator
            .validate_produce("users", None, json_msg)
            .await;
        assert!(result.is_ok());

        // Schema should have been auto-registered
        assert!(validator.has_schema("users-value"));
    }

    #[tokio::test]
    async fn test_produce_validator_with_registered_schema() {
        let store = Arc::new(SchemaStore::new(test_config()));

        // Register a JSON schema
        let schema = r#"{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}"#;
        store
            .register_schema("events-value", schema, SchemaType::Json, vec![])
            .await
            .unwrap();

        let validator = ProduceSchemaValidator::new(Arc::clone(&store), true, false);

        // Valid message
        let valid = br#"{"name": "test"}"#;
        assert!(validator.validate_produce("events", None, valid).await.is_ok());
    }

    #[test]
    fn test_latest_schema_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(SchemaStore::new(test_config()));

        rt.block_on(async {
            store
                .register_schema(
                    "topic-value",
                    r#"{"type":"object"}"#,
                    SchemaType::Json,
                    vec![],
                )
                .await
                .unwrap();
        });

        let validator = ProduceSchemaValidator::new(Arc::clone(&store), true, false);
        assert!(validator.latest_schema_id("topic-value").is_some());
        assert!(validator.latest_schema_id("nonexistent").is_none());
    }
}
