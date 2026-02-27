//! Schema Data Catalog & Lineage Tracker
//!
//! Provides a data catalog backend for schema lineage tracking, dependency graphs,
//! impact analysis, and team ownership metadata.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::error::{Result, StreamlineError};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Central catalog for schema metadata, ownership, and lineage.
#[derive(Debug, Clone)]
pub struct SchemaCatalog {
    entries: Arc<RwLock<HashMap<String, CatalogEntry>>>,
    lineage: Arc<RwLock<LineageGraph>>,
}

/// A single catalog entry describing a schema subject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub subject: String,
    pub description: Option<String>,
    pub owner: Option<OwnerInfo>,
    pub tags: Vec<String>,
    pub classification: DataClassification,
    /// Quality score in the range 0.0–1.0.
    pub quality_score: Option<f64>,
    pub created_at: u64,
    pub updated_at: u64,
    pub version_count: u32,
    /// Downstream subjects or services that consume this schema.
    pub consumers: Vec<String>,
    /// Upstream subjects or services that produce data for this schema.
    pub producers: Vec<String>,
    pub deprecation: Option<DeprecationInfo>,
}

/// Team / owner metadata attached to a catalog entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnerInfo {
    pub team: String,
    pub contact: String,
    pub slack_channel: Option<String>,
}

/// Data sensitivity classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum DataClassification {
    #[default]
    Public,
    Internal,
    Confidential,
    Restricted,
}

impl std::fmt::Display for DataClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataClassification::Public => write!(f, "Public"),
            DataClassification::Internal => write!(f, "Internal"),
            DataClassification::Confidential => write!(f, "Confidential"),
            DataClassification::Restricted => write!(f, "Restricted"),
        }
    }
}

/// Deprecation metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecationInfo {
    pub deprecated_at: u64,
    pub sunset_at: Option<u64>,
    pub replacement: Option<String>,
    pub reason: String,
}

/// Directed graph of lineage edges between subjects/services.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageGraph {
    edges: Vec<LineageEdge>,
}

/// A single directed edge in the lineage graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Source subject or service name.
    pub source: String,
    /// Target subject or service name.
    pub target: String,
    pub edge_type: LineageEdgeType,
    pub metadata: HashMap<String, String>,
}

/// The kind of relationship an edge represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LineageEdgeType {
    /// A service produces data to a topic schema.
    ProducesTo,
    /// A topic schema is consumed by a service.
    ConsumesFrom,
    /// Schema B was derived from schema A.
    DerivedFrom,
    /// Schema references another schema.
    References,
}

/// Result of an impact analysis starting from a given subject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactAnalysis {
    pub subject: String,
    pub direct_consumers: Vec<String>,
    pub transitive_consumers: Vec<String>,
    pub breaking_change_risk: BreakingChangeRisk,
    pub affected_services: Vec<String>,
}

/// Risk level for a potential breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BreakingChangeRisk {
    None,
    Low,
    Medium,
    High,
    Critical,
}

// ---------------------------------------------------------------------------
// Query / update helpers
// ---------------------------------------------------------------------------

/// Filter criteria for listing catalog entries.
#[derive(Debug, Clone, Default)]
pub struct CatalogFilter {
    pub owner_team: Option<String>,
    pub classification: Option<DataClassification>,
    pub tags: Option<Vec<String>>,
    pub deprecated: Option<bool>,
}

/// Partial update payload for a catalog entry.
#[derive(Debug, Clone, Default)]
pub struct CatalogEntryUpdate {
    pub description: Option<String>,
    pub owner: Option<OwnerInfo>,
    pub tags: Option<Vec<String>>,
    pub classification: Option<DataClassification>,
}

/// Upstream and downstream edges for a particular subject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageView {
    pub upstream: Vec<LineageEdge>,
    pub downstream: Vec<LineageEdge>,
}

/// Aggregate statistics about the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogStats {
    pub total_entries: usize,
    pub by_classification: HashMap<String, usize>,
    pub deprecated_count: usize,
    pub avg_quality_score: f64,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl SchemaCatalog {
    /// Create a new, empty catalog.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            lineage: Arc::new(RwLock::new(LineageGraph::default())),
        }
    }

    /// Register a new catalog entry. Fails if the subject already exists.
    pub fn register_entry(&self, entry: CatalogEntry) -> Result<()> {
        let mut entries = self.entries.write();
        if entries.contains_key(&entry.subject) {
            warn!(subject = %entry.subject, "catalog entry already exists");
            return Err(StreamlineError::Storage(format!(
                "Catalog entry already exists: {}",
                entry.subject
            )));
        }
        info!(subject = %entry.subject, "registered catalog entry");
        entries.insert(entry.subject.clone(), entry);
        Ok(())
    }

    /// Retrieve a catalog entry by subject name.
    pub fn get_entry(&self, subject: &str) -> Option<CatalogEntry> {
        self.entries.read().get(subject).cloned()
    }

    /// Apply a partial update to an existing entry.
    pub fn update_entry(&self, subject: &str, update: CatalogEntryUpdate) -> Result<()> {
        let mut entries = self.entries.write();
        let entry = entries.get_mut(subject).ok_or_else(|| {
            StreamlineError::Storage(format!("Catalog entry not found: {}", subject))
        })?;

        if let Some(desc) = update.description {
            entry.description = Some(desc);
        }
        if let Some(owner) = update.owner {
            entry.owner = Some(owner);
        }
        if let Some(tags) = update.tags {
            entry.tags = tags;
        }
        if let Some(cls) = update.classification {
            entry.classification = cls;
        }

        debug!(subject, "updated catalog entry");
        Ok(())
    }

    /// List entries, optionally filtered.
    pub fn list_entries(&self, filter: Option<CatalogFilter>) -> Vec<CatalogEntry> {
        let entries = self.entries.read();
        let iter = entries.values();

        match filter {
            None => iter.cloned().collect(),
            Some(f) => iter
                .filter(|e| {
                    if let Some(ref team) = f.owner_team {
                        if e.owner.as_ref().map(|o| &o.team) != Some(team) {
                            return false;
                        }
                    }
                    if let Some(cls) = f.classification {
                        if e.classification != cls {
                            return false;
                        }
                    }
                    if let Some(ref tags) = f.tags {
                        if !tags.iter().all(|t| e.tags.contains(t)) {
                            return false;
                        }
                    }
                    if let Some(dep) = f.deprecated {
                        if e.deprecation.is_some() != dep {
                            return false;
                        }
                    }
                    true
                })
                .cloned()
                .collect(),
        }
    }

    /// Delete a catalog entry by subject name.
    pub fn delete_entry(&self, subject: &str) -> Result<()> {
        let mut entries = self.entries.write();
        if entries.remove(subject).is_none() {
            return Err(StreamlineError::Storage(format!(
                "Catalog entry not found: {}",
                subject
            )));
        }
        info!(subject, "deleted catalog entry");
        Ok(())
    }

    // -- Lineage ----------------------------------------------------------

    /// Add a directed edge to the lineage graph.
    pub fn add_lineage_edge(&self, edge: LineageEdge) -> Result<()> {
        debug!(
            source = %edge.source,
            target = %edge.target,
            edge_type = ?edge.edge_type,
            "adding lineage edge"
        );
        self.lineage.write().edges.push(edge);
        Ok(())
    }

    /// Get upstream and downstream edges for a given subject.
    pub fn get_lineage(&self, subject: &str) -> LineageView {
        let graph = self.lineage.read();
        let upstream = graph
            .edges
            .iter()
            .filter(|e| e.target == subject)
            .cloned()
            .collect();
        let downstream = graph
            .edges
            .iter()
            .filter(|e| e.source == subject)
            .cloned()
            .collect();
        LineageView {
            upstream,
            downstream,
        }
    }

    /// Perform impact analysis: BFS through `ConsumesFrom` and `DerivedFrom`
    /// edges to find all transitively affected nodes.
    pub fn analyze_impact(&self, subject: &str) -> ImpactAnalysis {
        let graph = self.lineage.read();

        // Direct consumers: targets of edges originating from `subject` with
        // type ConsumesFrom, or sources of edges targeting `subject` with
        // ConsumesFrom (i.e. someone consuming from subject).
        // Convention: an edge (subject -> consumer) with ConsumesFrom means
        // `consumer` consumes from `subject`. We also consider DerivedFrom
        // edges where target == subject (source derived from subject).
        //
        // For traversal we follow outgoing ConsumesFrom and outgoing DerivedFrom
        // edges (source == current node).

        let direct: Vec<String> = graph
            .edges
            .iter()
            .filter(|e| {
                e.source == subject
                    && matches!(
                        e.edge_type,
                        LineageEdgeType::ConsumesFrom | LineageEdgeType::DerivedFrom
                    )
            })
            .map(|e| e.target.clone())
            .collect();

        // BFS for transitive consumers
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut transitive: Vec<String> = Vec::new();

        for d in &direct {
            if visited.insert(d.clone()) {
                queue.push_back(d.clone());
            }
        }

        while let Some(node) = queue.pop_front() {
            for edge in &graph.edges {
                if edge.source == node
                    && matches!(
                        edge.edge_type,
                        LineageEdgeType::ConsumesFrom | LineageEdgeType::DerivedFrom
                    )
                {
                    if visited.insert(edge.target.clone()) {
                        transitive.push(edge.target.clone());
                        queue.push_back(edge.target.clone());
                    }
                }
            }
        }

        // Affected services = direct + transitive (de-duped)
        let mut affected: Vec<String> = direct.clone();
        affected.extend(transitive.iter().cloned());
        affected.sort();
        affected.dedup();

        let risk = match affected.len() {
            0 => BreakingChangeRisk::None,
            1 => BreakingChangeRisk::Low,
            2..=4 => BreakingChangeRisk::Medium,
            5..=9 => BreakingChangeRisk::High,
            _ => BreakingChangeRisk::Critical,
        };

        ImpactAnalysis {
            subject: subject.to_string(),
            direct_consumers: direct,
            transitive_consumers: transitive,
            breaking_change_risk: risk,
            affected_services: affected,
        }
    }

    /// Case-insensitive substring search across subject, description, tags,
    /// and owner team.
    pub fn search(&self, query: &str) -> Vec<CatalogEntry> {
        let q = query.to_lowercase();
        self.entries
            .read()
            .values()
            .filter(|e| {
                if e.subject.to_lowercase().contains(&q) {
                    return true;
                }
                if let Some(ref desc) = e.description {
                    if desc.to_lowercase().contains(&q) {
                        return true;
                    }
                }
                if e.tags
                    .iter()
                    .any(|t| t.to_lowercase().contains(&q))
                {
                    return true;
                }
                if let Some(ref owner) = e.owner {
                    if owner.team.to_lowercase().contains(&q) {
                        return true;
                    }
                }
                false
            })
            .cloned()
            .collect()
    }

    /// Mark a catalog entry as deprecated.
    pub fn deprecate(&self, subject: &str, info: DeprecationInfo) -> Result<()> {
        let mut entries = self.entries.write();
        let entry = entries.get_mut(subject).ok_or_else(|| {
            StreamlineError::Storage(format!("Catalog entry not found: {}", subject))
        })?;
        warn!(
            subject,
            reason = %info.reason,
            "deprecating catalog entry"
        );
        entry.deprecation = Some(info);
        Ok(())
    }

    /// Aggregate statistics about the catalog.
    pub fn stats(&self) -> CatalogStats {
        let entries = self.entries.read();
        let total = entries.len();
        let mut by_classification: HashMap<String, usize> = HashMap::new();
        let mut deprecated_count: usize = 0;
        let mut quality_sum: f64 = 0.0;
        let mut quality_count: usize = 0;

        for e in entries.values() {
            *by_classification
                .entry(e.classification.to_string())
                .or_insert(0) += 1;
            if e.deprecation.is_some() {
                deprecated_count += 1;
            }
            if let Some(q) = e.quality_score {
                quality_sum += q;
                quality_count += 1;
            }
        }

        CatalogStats {
            total_entries: total,
            by_classification,
            deprecated_count,
            avg_quality_score: if quality_count > 0 {
                quality_sum / quality_count as f64
            } else {
                0.0
            },
        }
    }

    /// Return all edges in the lineage graph.
    pub fn get_all_edges(&self) -> Vec<LineageEdge> {
        self.lineage.read().edges.clone()
    }

    /// Remove a directed edge between `source` and `target`.
    pub fn remove_lineage_edge(&self, source: &str, target: &str) -> Result<()> {
        let mut graph = self.lineage.write();
        let before = graph.edges.len();
        graph.edges.retain(|e| !(e.source == source && e.target == target));
        if graph.edges.len() == before {
            return Err(StreamlineError::Storage(format!(
                "Lineage edge not found: {} -> {}",
                source, target
            )));
        }
        info!(source, target, "removed lineage edge");
        Ok(())
    }
}

impl Default for SchemaCatalog {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helper to build entries quickly (used by tests)
// ---------------------------------------------------------------------------

fn make_entry(subject: &str) -> CatalogEntry {
    CatalogEntry {
        subject: subject.to_string(),
        description: None,
        owner: None,
        tags: Vec::new(),
        classification: DataClassification::Public,
        quality_score: None,
        created_at: 0,
        updated_at: 0,
        version_count: 1,
        consumers: Vec::new(),
        producers: Vec::new(),
        deprecation: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(subject: &str) -> CatalogEntry {
        make_entry(subject)
    }

    fn entry_with_owner(subject: &str, team: &str) -> CatalogEntry {
        let mut e = sample_entry(subject);
        e.owner = Some(OwnerInfo {
            team: team.to_string(),
            contact: format!("{}@example.com", team),
            slack_channel: None,
        });
        e
    }

    // -- CRUD tests -------------------------------------------------------

    #[test]
    fn test_register_and_get_entry() {
        let catalog = SchemaCatalog::new();
        let entry = sample_entry("orders-value");
        catalog.register_entry(entry.clone()).unwrap();

        let got = catalog.get_entry("orders-value").unwrap();
        assert_eq!(got.subject, "orders-value");
    }

    #[test]
    fn test_register_duplicate_fails() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("dup")).unwrap();
        assert!(catalog.register_entry(sample_entry("dup")).is_err());
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let catalog = SchemaCatalog::new();
        assert!(catalog.get_entry("nope").is_none());
    }

    #[test]
    fn test_update_entry() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("upd")).unwrap();
        catalog
            .update_entry(
                "upd",
                CatalogEntryUpdate {
                    description: Some("updated desc".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let e = catalog.get_entry("upd").unwrap();
        assert_eq!(e.description.as_deref(), Some("updated desc"));
    }

    #[test]
    fn test_update_nonexistent_fails() {
        let catalog = SchemaCatalog::new();
        assert!(catalog
            .update_entry("missing", CatalogEntryUpdate::default())
            .is_err());
    }

    #[test]
    fn test_delete_entry() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("del")).unwrap();
        catalog.delete_entry("del").unwrap();
        assert!(catalog.get_entry("del").is_none());
    }

    #[test]
    fn test_delete_nonexistent_fails() {
        let catalog = SchemaCatalog::new();
        assert!(catalog.delete_entry("ghost").is_err());
    }

    #[test]
    fn test_list_entries_no_filter() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("a")).unwrap();
        catalog.register_entry(sample_entry("b")).unwrap();
        let list = catalog.list_entries(None);
        assert_eq!(list.len(), 2);
    }

    // -- Filtering --------------------------------------------------------

    #[test]
    fn test_filter_by_owner_team() {
        let catalog = SchemaCatalog::new();
        catalog
            .register_entry(entry_with_owner("s1", "platform"))
            .unwrap();
        catalog
            .register_entry(entry_with_owner("s2", "data"))
            .unwrap();

        let filter = CatalogFilter {
            owner_team: Some("platform".into()),
            ..Default::default()
        };
        let results = catalog.list_entries(Some(filter));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "s1");
    }

    #[test]
    fn test_filter_by_classification() {
        let catalog = SchemaCatalog::new();
        let mut e = sample_entry("conf");
        e.classification = DataClassification::Confidential;
        catalog.register_entry(e).unwrap();
        catalog.register_entry(sample_entry("pub")).unwrap();

        let filter = CatalogFilter {
            classification: Some(DataClassification::Confidential),
            ..Default::default()
        };
        let results = catalog.list_entries(Some(filter));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "conf");
    }

    #[test]
    fn test_filter_by_tags() {
        let catalog = SchemaCatalog::new();
        let mut e = sample_entry("tagged");
        e.tags = vec!["pii".into(), "gdpr".into()];
        catalog.register_entry(e).unwrap();
        catalog.register_entry(sample_entry("untagged")).unwrap();

        let filter = CatalogFilter {
            tags: Some(vec!["pii".into()]),
            ..Default::default()
        };
        let results = catalog.list_entries(Some(filter));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "tagged");
    }

    #[test]
    fn test_filter_by_deprecated() {
        let catalog = SchemaCatalog::new();
        let mut e = sample_entry("old");
        e.deprecation = Some(DeprecationInfo {
            deprecated_at: 1000,
            sunset_at: None,
            replacement: None,
            reason: "outdated".into(),
        });
        catalog.register_entry(e).unwrap();
        catalog.register_entry(sample_entry("new")).unwrap();

        let filter = CatalogFilter {
            deprecated: Some(true),
            ..Default::default()
        };
        let results = catalog.list_entries(Some(filter));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "old");
    }

    // -- Lineage ----------------------------------------------------------

    #[test]
    fn test_add_and_get_lineage() {
        let catalog = SchemaCatalog::new();
        catalog
            .add_lineage_edge(LineageEdge {
                source: "svc-a".into(),
                target: "topic-x".into(),
                edge_type: LineageEdgeType::ProducesTo,
                metadata: HashMap::new(),
            })
            .unwrap();
        catalog
            .add_lineage_edge(LineageEdge {
                source: "topic-x".into(),
                target: "svc-b".into(),
                edge_type: LineageEdgeType::ConsumesFrom,
                metadata: HashMap::new(),
            })
            .unwrap();

        let view = catalog.get_lineage("topic-x");
        assert_eq!(view.upstream.len(), 1);
        assert_eq!(view.upstream[0].source, "svc-a");
        assert_eq!(view.downstream.len(), 1);
        assert_eq!(view.downstream[0].target, "svc-b");
    }

    #[test]
    fn test_lineage_empty_subject() {
        let catalog = SchemaCatalog::new();
        let view = catalog.get_lineage("nothing");
        assert!(view.upstream.is_empty());
        assert!(view.downstream.is_empty());
    }

    // -- Impact analysis --------------------------------------------------

    #[test]
    fn test_impact_analysis_no_consumers() {
        let catalog = SchemaCatalog::new();
        let impact = catalog.analyze_impact("lonely");
        assert!(impact.direct_consumers.is_empty());
        assert!(impact.transitive_consumers.is_empty());
        assert_eq!(impact.breaking_change_risk, BreakingChangeRisk::None);
    }

    #[test]
    fn test_impact_analysis_direct() {
        let catalog = SchemaCatalog::new();
        catalog
            .add_lineage_edge(LineageEdge {
                source: "schema-a".into(),
                target: "svc-1".into(),
                edge_type: LineageEdgeType::ConsumesFrom,
                metadata: HashMap::new(),
            })
            .unwrap();

        let impact = catalog.analyze_impact("schema-a");
        assert_eq!(impact.direct_consumers, vec!["svc-1"]);
        assert_eq!(impact.breaking_change_risk, BreakingChangeRisk::Low);
    }

    #[test]
    fn test_impact_analysis_transitive() {
        let catalog = SchemaCatalog::new();
        // schema-a -> svc-1 (ConsumesFrom)
        catalog
            .add_lineage_edge(LineageEdge {
                source: "schema-a".into(),
                target: "svc-1".into(),
                edge_type: LineageEdgeType::ConsumesFrom,
                metadata: HashMap::new(),
            })
            .unwrap();
        // svc-1 -> svc-2 (DerivedFrom)
        catalog
            .add_lineage_edge(LineageEdge {
                source: "svc-1".into(),
                target: "svc-2".into(),
                edge_type: LineageEdgeType::DerivedFrom,
                metadata: HashMap::new(),
            })
            .unwrap();
        // svc-2 -> svc-3 (ConsumesFrom)
        catalog
            .add_lineage_edge(LineageEdge {
                source: "svc-2".into(),
                target: "svc-3".into(),
                edge_type: LineageEdgeType::ConsumesFrom,
                metadata: HashMap::new(),
            })
            .unwrap();

        let impact = catalog.analyze_impact("schema-a");
        assert_eq!(impact.direct_consumers, vec!["svc-1"]);
        assert!(impact.transitive_consumers.contains(&"svc-2".to_string()));
        assert!(impact.transitive_consumers.contains(&"svc-3".to_string()));
        assert_eq!(impact.affected_services.len(), 3);
        assert_eq!(impact.breaking_change_risk, BreakingChangeRisk::Medium);
    }

    #[test]
    fn test_impact_analysis_high_risk() {
        let catalog = SchemaCatalog::new();
        for i in 0..6 {
            catalog
                .add_lineage_edge(LineageEdge {
                    source: "root".into(),
                    target: format!("svc-{}", i),
                    edge_type: LineageEdgeType::ConsumesFrom,
                    metadata: HashMap::new(),
                })
                .unwrap();
        }
        let impact = catalog.analyze_impact("root");
        assert_eq!(impact.breaking_change_risk, BreakingChangeRisk::High);
    }

    #[test]
    fn test_impact_analysis_critical_risk() {
        let catalog = SchemaCatalog::new();
        for i in 0..11 {
            catalog
                .add_lineage_edge(LineageEdge {
                    source: "hub".into(),
                    target: format!("consumer-{}", i),
                    edge_type: LineageEdgeType::ConsumesFrom,
                    metadata: HashMap::new(),
                })
                .unwrap();
        }
        let impact = catalog.analyze_impact("hub");
        assert_eq!(impact.breaking_change_risk, BreakingChangeRisk::Critical);
    }

    // -- Search -----------------------------------------------------------

    #[test]
    fn test_search_by_subject() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("user-events")).unwrap();
        catalog.register_entry(sample_entry("order-events")).unwrap();
        let results = catalog.search("user");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "user-events");
    }

    #[test]
    fn test_search_case_insensitive() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("UserEvents")).unwrap();
        let results = catalog.search("userevents");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_by_description() {
        let catalog = SchemaCatalog::new();
        let mut e = sample_entry("x");
        e.description = Some("Tracks user signups".into());
        catalog.register_entry(e).unwrap();
        let results = catalog.search("signup");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_by_tag() {
        let catalog = SchemaCatalog::new();
        let mut e = sample_entry("y");
        e.tags = vec!["analytics".into()];
        catalog.register_entry(e).unwrap();
        let results = catalog.search("analytics");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_by_owner_team() {
        let catalog = SchemaCatalog::new();
        catalog
            .register_entry(entry_with_owner("z", "payments"))
            .unwrap();
        let results = catalog.search("payments");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_no_results() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("abc")).unwrap();
        let results = catalog.search("zzz");
        assert!(results.is_empty());
    }

    // -- Deprecation ------------------------------------------------------

    #[test]
    fn test_deprecate_entry() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("old-schema")).unwrap();
        catalog
            .deprecate(
                "old-schema",
                DeprecationInfo {
                    deprecated_at: 1700000000,
                    sunset_at: Some(1703000000),
                    replacement: Some("new-schema".into()),
                    reason: "migrating to v2".into(),
                },
            )
            .unwrap();

        let e = catalog.get_entry("old-schema").unwrap();
        let dep = e.deprecation.unwrap();
        assert_eq!(dep.reason, "migrating to v2");
        assert_eq!(dep.replacement.as_deref(), Some("new-schema"));
    }

    #[test]
    fn test_deprecate_nonexistent_fails() {
        let catalog = SchemaCatalog::new();
        assert!(catalog
            .deprecate(
                "nope",
                DeprecationInfo {
                    deprecated_at: 0,
                    sunset_at: None,
                    replacement: None,
                    reason: "n/a".into(),
                },
            )
            .is_err());
    }

    // -- Stats ------------------------------------------------------------

    #[test]
    fn test_stats_empty() {
        let catalog = SchemaCatalog::new();
        let s = catalog.stats();
        assert_eq!(s.total_entries, 0);
        assert_eq!(s.deprecated_count, 0);
        assert_eq!(s.avg_quality_score, 0.0);
    }

    #[test]
    fn test_stats_counts() {
        let catalog = SchemaCatalog::new();

        let mut e1 = sample_entry("s1");
        e1.classification = DataClassification::Internal;
        e1.quality_score = Some(0.8);

        let mut e2 = sample_entry("s2");
        e2.classification = DataClassification::Internal;
        e2.quality_score = Some(0.6);
        e2.deprecation = Some(DeprecationInfo {
            deprecated_at: 1,
            sunset_at: None,
            replacement: None,
            reason: "old".into(),
        });

        let mut e3 = sample_entry("s3");
        e3.classification = DataClassification::Restricted;

        catalog.register_entry(e1).unwrap();
        catalog.register_entry(e2).unwrap();
        catalog.register_entry(e3).unwrap();

        let s = catalog.stats();
        assert_eq!(s.total_entries, 3);
        assert_eq!(s.deprecated_count, 1);
        assert_eq!(*s.by_classification.get("Internal").unwrap(), 2);
        assert_eq!(*s.by_classification.get("Restricted").unwrap(), 1);
        assert!((s.avg_quality_score - 0.7).abs() < 1e-9);
    }

    // -- Update with multiple fields --------------------------------------

    #[test]
    fn test_update_multiple_fields() {
        let catalog = SchemaCatalog::new();
        catalog.register_entry(sample_entry("multi")).unwrap();
        catalog
            .update_entry(
                "multi",
                CatalogEntryUpdate {
                    description: Some("new desc".into()),
                    tags: Some(vec!["t1".into(), "t2".into()]),
                    classification: Some(DataClassification::Restricted),
                    owner: Some(OwnerInfo {
                        team: "infra".into(),
                        contact: "infra@co.com".into(),
                        slack_channel: Some("#infra".into()),
                    }),
                },
            )
            .unwrap();

        let e = catalog.get_entry("multi").unwrap();
        assert_eq!(e.description.as_deref(), Some("new desc"));
        assert_eq!(e.tags, vec!["t1", "t2"]);
        assert_eq!(e.classification, DataClassification::Restricted);
        assert_eq!(e.owner.as_ref().unwrap().team, "infra");
        assert_eq!(
            e.owner.as_ref().unwrap().slack_channel.as_deref(),
            Some("#infra")
        );
    }

    // -- Lineage edge types -----------------------------------------------

    #[test]
    fn test_lineage_references_edge() {
        let catalog = SchemaCatalog::new();
        catalog
            .add_lineage_edge(LineageEdge {
                source: "common.avro".into(),
                target: "order.avro".into(),
                edge_type: LineageEdgeType::References,
                metadata: HashMap::from([("field".into(), "address".into())]),
            })
            .unwrap();

        let view = catalog.get_lineage("order.avro");
        assert_eq!(view.upstream.len(), 1);
        assert_eq!(view.upstream[0].edge_type, LineageEdgeType::References);
        assert_eq!(view.upstream[0].metadata.get("field").unwrap(), "address");
    }

    // -- Default trait ----------------------------------------------------

    #[test]
    fn test_default_catalog() {
        let catalog = SchemaCatalog::default();
        assert_eq!(catalog.list_entries(None).len(), 0);
    }
}
