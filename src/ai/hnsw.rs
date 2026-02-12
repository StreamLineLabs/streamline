//! HNSW (Hierarchical Navigable Small World) Vector Index
//!
//! Provides efficient approximate nearest neighbor search for high-dimensional vectors.
//! Based on the paper "Efficient and robust approximate nearest neighbor search using
//! Hierarchical Navigable Small World graphs" by Malkov and Yashunin.

use crate::error::{Result, StreamlineError};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Distance metric for vector comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine distance (1 - cosine_similarity)
    #[default]
    Cosine,
    /// Euclidean (L2) distance
    Euclidean,
    /// Dot product (negative for max-similarity)
    DotProduct,
}

/// HNSW index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Number of connections per layer (M parameter)
    pub m: usize,
    /// Maximum connections at layer 0 (M0 = 2 * M by default)
    pub m0: usize,
    /// Number of candidates during construction (ef_construction)
    pub ef_construction: usize,
    /// Number of candidates during search (ef_search)
    pub ef_search: usize,
    /// Vector dimension
    pub dimension: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Probability factor for level generation (1/ln(M))
    pub level_mult: f64,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            m,
            m0: m * 2,
            ef_construction: 200,
            ef_search: 100,
            dimension: 384,
            metric: DistanceMetric::Cosine,
            level_mult: 1.0 / (m as f64).ln(),
        }
    }
}

impl HnswConfig {
    /// Create configuration optimized for speed
    pub fn fast(dimension: usize) -> Self {
        Self {
            m: 12,
            m0: 24,
            ef_construction: 100,
            ef_search: 50,
            dimension,
            metric: DistanceMetric::Cosine,
            level_mult: 1.0 / 12.0_f64.ln(),
        }
    }

    /// Create configuration optimized for accuracy
    pub fn accurate(dimension: usize) -> Self {
        Self {
            m: 32,
            m0: 64,
            ef_construction: 400,
            ef_search: 200,
            dimension,
            metric: DistanceMetric::Cosine,
            level_mult: 1.0 / 32.0_f64.ln(),
        }
    }

    /// Create configuration balanced for production use
    pub fn balanced(dimension: usize) -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            ef_search: 100,
            dimension,
            metric: DistanceMetric::Cosine,
            level_mult: 1.0 / 16.0_f64.ln(),
        }
    }
}

/// A node in the HNSW graph
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct HnswNode {
    /// Unique node ID (typically record offset)
    id: i64,
    /// Vector data
    vector: Vec<f32>,
    /// Connections at each level (level -> neighbor IDs)
    neighbors: Vec<Vec<i64>>,
    /// Maximum level for this node
    max_level: usize,
}

impl HnswNode {
    fn new(id: i64, vector: Vec<f32>, max_level: usize, m: usize, m0: usize) -> Self {
        let mut neighbors = Vec::with_capacity(max_level + 1);
        for level in 0..=max_level {
            let capacity = if level == 0 { m0 } else { m };
            neighbors.push(Vec::with_capacity(capacity));
        }
        Self {
            id,
            vector,
            neighbors,
            max_level,
        }
    }
}

/// Search result with distance
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Node ID
    pub id: i64,
    /// Distance from query
    pub distance: f32,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// Thread-safe HNSW index
pub struct HnswIndex {
    config: HnswConfig,
    nodes: Arc<RwLock<HashMap<i64, HnswNode>>>,
    entry_point: Arc<RwLock<Option<i64>>>,
    max_level: Arc<RwLock<usize>>,
    stats: HnswStats,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            entry_point: Arc::new(RwLock::new(None)),
            max_level: Arc::new(RwLock::new(0)),
            stats: HnswStats::default(),
        }
    }

    /// Insert a vector into the index
    pub async fn insert(&self, id: i64, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.config.dimension {
            return Err(StreamlineError::InvalidData(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimension,
                vector.len()
            )));
        }

        self.stats.insertions.fetch_add(1, AtomicOrdering::Relaxed);

        // Generate random level for this node
        let node_level = self.random_level();

        let node = HnswNode::new(
            id,
            vector.clone(),
            node_level,
            self.config.m,
            self.config.m0,
        );

        // Get current state
        let entry_point = *self.entry_point.read().await;
        let current_max_level = *self.max_level.read().await;

        // Add node to storage
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(id, node);
        }

        // If this is the first node, set it as entry point
        if entry_point.is_none() {
            let mut ep = self.entry_point.write().await;
            *ep = Some(id);
            let mut ml = self.max_level.write().await;
            *ml = node_level;
            return Ok(());
        }

        let entry_point_id = entry_point.ok_or_else(|| {
            StreamlineError::InvalidData("No entry point found".into())
        })?;

        // Search from top level to node's level + 1
        let mut current_ep = entry_point_id;
        for level in (node_level + 1..=current_max_level).rev() {
            let neighbors = self.search_layer(&vector, current_ep, 1, level).await?;
            if let Some(nearest) = neighbors.first() {
                current_ep = nearest.id;
            }
        }

        // Insert at each level from node_level down to 0
        for level in (0..=node_level.min(current_max_level)).rev() {
            // Find ef_construction nearest neighbors at this level
            let neighbors = self
                .search_layer(&vector, current_ep, self.config.ef_construction, level)
                .await?;

            // Select M (or M0 for level 0) neighbors
            let max_neighbors = if level == 0 {
                self.config.m0
            } else {
                self.config.m
            };
            let selected: Vec<i64> = neighbors
                .into_iter()
                .take(max_neighbors)
                .map(|r| r.id)
                .collect();

            // Add bidirectional connections
            {
                let mut nodes = self.nodes.write().await;

                // Add connections from new node to selected neighbors
                if let Some(node) = nodes.get_mut(&id) {
                    node.neighbors[level] = selected.clone();
                }

                // Add connections from neighbors to new node
                for &neighbor_id in &selected {
                    // First check if we need to prune and collect necessary data
                    let prune_info = if let Some(neighbor) = nodes.get(&neighbor_id) {
                        if neighbor.neighbors.len() > level {
                            let current_count = neighbor.neighbors[level].len();
                            if current_count >= max_neighbors {
                                // Need to collect candidates for pruning
                                let neighbor_vec = neighbor.vector.clone();
                                let neighbor_ids: Vec<i64> = neighbor.neighbors[level].clone();
                                Some((neighbor_vec, neighbor_ids))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        continue;
                    };

                    // Now we can get mutable access
                    if let Some(neighbor) = nodes.get_mut(&neighbor_id) {
                        if neighbor.neighbors.len() > level {
                            neighbor.neighbors[level].push(id);

                            // Prune if necessary
                            if let Some((neighbor_vec, neighbor_ids)) = prune_info {
                                let mut candidates: Vec<SearchResult> = neighbor_ids
                                    .iter()
                                    .filter_map(|&nid| {
                                        nodes.get(&nid).map(|n| SearchResult {
                                            id: nid,
                                            distance: self.distance(&neighbor_vec, &n.vector),
                                        })
                                    })
                                    .collect();
                                // Add the newly added node to candidates
                                if let Some(new_node) = nodes.get(&id) {
                                    candidates.push(SearchResult {
                                        id,
                                        distance: self.distance(&neighbor_vec, &new_node.vector),
                                    });
                                }
                                candidates.sort_by(|a, b| {
                                    a.distance
                                        .partial_cmp(&b.distance)
                                        .unwrap_or(Ordering::Equal)
                                });
                                // Re-borrow mutably to update
                                if let Some(neighbor) = nodes.get_mut(&neighbor_id) {
                                    neighbor.neighbors[level] = candidates
                                        .into_iter()
                                        .take(max_neighbors)
                                        .map(|r| r.id)
                                        .collect();
                                }
                            }
                        }
                    }
                }
            }

            if !selected.is_empty() {
                current_ep = selected[0];
            }
        }

        // Update entry point if new node has higher level
        if node_level > current_max_level {
            let mut ep = self.entry_point.write().await;
            *ep = Some(id);
            let mut ml = self.max_level.write().await;
            *ml = node_level;
        }

        Ok(())
    }

    /// Search for k nearest neighbors
    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        if query.len() != self.config.dimension {
            return Err(StreamlineError::InvalidData(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimension,
                query.len()
            )));
        }

        self.stats.searches.fetch_add(1, AtomicOrdering::Relaxed);

        let entry_point = *self.entry_point.read().await;
        let entry_point_id = match entry_point {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let max_level = *self.max_level.read().await;

        // Greedy search from top to level 1
        let mut current_ep = entry_point_id;
        for level in (1..=max_level).rev() {
            let neighbors = self.search_layer(query, current_ep, 1, level).await?;
            if let Some(nearest) = neighbors.first() {
                current_ep = nearest.id;
            }
        }

        // Search at level 0 with ef_search candidates
        let mut results = self
            .search_layer(query, current_ep, self.config.ef_search, 0)
            .await?;

        // Return top k
        results.truncate(k);
        Ok(results)
    }

    /// Search a single layer
    async fn search_layer(
        &self,
        query: &[f32],
        entry_point: i64,
        ef: usize,
        level: usize,
    ) -> Result<Vec<SearchResult>> {
        let nodes = self.nodes.read().await;

        let ep_node = nodes.get(&entry_point).ok_or_else(|| {
            StreamlineError::InvalidData(format!("Entry point {} not found", entry_point))
        })?;

        let ep_dist = self.distance(query, &ep_node.vector);

        let mut visited = HashSet::new();
        visited.insert(entry_point);

        // Candidates (min-heap by distance)
        let mut candidates: BinaryHeap<SearchResult> = BinaryHeap::new();
        candidates.push(SearchResult {
            id: entry_point,
            distance: ep_dist,
        });

        // Results (max-heap by distance, we want to keep smallest)
        let mut results: BinaryHeap<std::cmp::Reverse<SearchResult>> = BinaryHeap::new();
        results.push(std::cmp::Reverse(SearchResult {
            id: entry_point,
            distance: ep_dist,
        }));

        while let Some(current) = candidates.pop() {
            // Get furthest result
            let furthest_dist = results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);

            // Stop if current is further than furthest result
            if current.distance > furthest_dist && results.len() >= ef {
                break;
            }

            // Get neighbors at this level
            if let Some(node) = nodes.get(&current.id) {
                if node.neighbors.len() > level {
                    for &neighbor_id in &node.neighbors[level] {
                        if visited.contains(&neighbor_id) {
                            continue;
                        }
                        visited.insert(neighbor_id);

                        if let Some(neighbor_node) = nodes.get(&neighbor_id) {
                            let dist = self.distance(query, &neighbor_node.vector);
                            self.stats
                                .distance_computations
                                .fetch_add(1, AtomicOrdering::Relaxed);

                            let furthest_dist =
                                results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);

                            if dist < furthest_dist || results.len() < ef {
                                candidates.push(SearchResult {
                                    id: neighbor_id,
                                    distance: dist,
                                });
                                results.push(std::cmp::Reverse(SearchResult {
                                    id: neighbor_id,
                                    distance: dist,
                                }));

                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert to sorted results (ascending by distance)
        let mut final_results: Vec<_> = results.into_iter().map(|r| r.0).collect();
        final_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        Ok(final_results)
    }

    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    1.0
                } else {
                    1.0 - (dot / (norm_a * norm_b))
                }
            }
            DistanceMetric::Euclidean => a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y).powi(2))
                .sum::<f32>()
                .sqrt(),
            DistanceMetric::DotProduct => {
                // Negative dot product so smaller = better match
                -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
        }
    }

    /// Generate random level for a new node
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let mut level = 0;
        while rng.gen::<f64>() < self.config.level_mult && level < 16 {
            level += 1;
        }
        level
    }

    /// Get number of nodes in the index
    pub async fn len(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Check if index is empty
    pub async fn is_empty(&self) -> bool {
        self.nodes.read().await.is_empty()
    }

    /// Get index statistics
    pub fn stats(&self) -> HnswStatsSnapshot {
        HnswStatsSnapshot {
            insertions: self.stats.insertions.load(AtomicOrdering::Relaxed),
            searches: self.stats.searches.load(AtomicOrdering::Relaxed),
            distance_computations: self
                .stats
                .distance_computations
                .load(AtomicOrdering::Relaxed),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    /// Remove a node from the index
    pub async fn remove(&self, id: i64) -> Result<bool> {
        let mut nodes = self.nodes.write().await;

        if !nodes.contains_key(&id) {
            return Ok(false);
        }

        // Get node's neighbors before removal
        let node_neighbors: Vec<Vec<i64>> = nodes
            .get(&id)
            .map(|n| n.neighbors.clone())
            .unwrap_or_default();

        // Remove references from neighbors
        for (level, level_neighbors) in node_neighbors.iter().enumerate() {
            for &neighbor_id in level_neighbors {
                if let Some(neighbor) = nodes.get_mut(&neighbor_id) {
                    if neighbor.neighbors.len() > level {
                        neighbor.neighbors[level].retain(|&x| x != id);
                    }
                }
            }
        }

        // Remove the node
        nodes.remove(&id);

        // Update entry point if needed
        let current_entry = *self.entry_point.read().await;
        if current_entry == Some(id) {
            let mut ep = self.entry_point.write().await;
            *ep = nodes.keys().next().copied();
        }

        Ok(true)
    }

    /// Save the index to disk in binary format.
    pub async fn save(&self, path: &std::path::Path) -> Result<()> {
        #[derive(Serialize)]
        struct IndexSnapshot {
            config: HnswConfig,
            nodes: HashMap<i64, HnswNode>,
            entry_point: Option<i64>,
            max_level: usize,
        }

        let nodes = self.nodes.read().await;
        let entry_point = *self.entry_point.read().await;
        let max_level = *self.max_level.read().await;

        let snapshot = IndexSnapshot {
            config: self.config.clone(),
            nodes: nodes.clone(),
            entry_point,
            max_level,
        };

        let data = bincode::serialize(&snapshot)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to serialize HNSW index: {}", e)))?;
        std::fs::write(path, data)?;

        tracing::info!(path = %path.display(), nodes = nodes.len(), "HNSW index saved to disk");
        Ok(())
    }

    /// Load the index from disk.
    pub async fn load(path: &std::path::Path) -> Result<Self> {
        #[derive(Deserialize)]
        struct IndexSnapshot {
            config: HnswConfig,
            nodes: HashMap<i64, HnswNode>,
            entry_point: Option<i64>,
            max_level: usize,
        }

        let data = std::fs::read(path)?;
        let snapshot: IndexSnapshot = bincode::deserialize(&data)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to deserialize HNSW index: {}", e)))?;

        let node_count = snapshot.nodes.len();
        let index = Self {
            config: snapshot.config,
            nodes: Arc::new(RwLock::new(snapshot.nodes)),
            entry_point: Arc::new(RwLock::new(snapshot.entry_point)),
            max_level: Arc::new(RwLock::new(snapshot.max_level)),
            stats: HnswStats::default(),
        };

        tracing::info!(path = %path.display(), nodes = node_count, "HNSW index loaded from disk");
        Ok(index)
    }
}

/// HNSW statistics
#[derive(Debug, Default)]
struct HnswStats {
    insertions: AtomicU64,
    searches: AtomicU64,
    distance_computations: AtomicU64,
}

/// Snapshot of HNSW statistics
#[derive(Debug, Clone, Default)]
pub struct HnswStatsSnapshot {
    /// Number of insertions
    pub insertions: u64,
    /// Number of searches
    pub searches: u64,
    /// Number of distance computations
    pub distance_computations: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vector(dim: usize) -> Vec<f32> {
        let mut rng = rand::thread_rng();
        (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect()
    }

    fn normalize(v: &mut [f32]) {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
    }

    #[tokio::test]
    async fn test_hnsw_insert_and_search() {
        let config = HnswConfig::fast(4);
        let index = HnswIndex::new(config);

        // Insert some vectors
        index.insert(1, vec![1.0, 0.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, vec![0.9, 0.1, 0.0, 0.0]).await.unwrap();
        index.insert(3, vec![0.0, 1.0, 0.0, 0.0]).await.unwrap();
        index.insert(4, vec![0.0, 0.0, 1.0, 0.0]).await.unwrap();

        // Search for nearest to [1, 0, 0, 0]
        let results = index.search(&[1.0, 0.0, 0.0, 0.0], 2).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1); // Exact match
        assert_eq!(results[1].id, 2); // Next closest
    }

    #[tokio::test]
    async fn test_hnsw_empty_search() {
        let config = HnswConfig::fast(4);
        let index = HnswIndex::new(config);

        let results = index.search(&[1.0, 0.0, 0.0, 0.0], 5).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_hnsw_many_vectors() {
        let dim = 32;
        let config = HnswConfig::balanced(dim);
        let index = HnswIndex::new(config);

        // Insert 100 random vectors
        for i in 0..100 {
            let mut v = random_vector(dim);
            normalize(&mut v);
            index.insert(i, v).await.unwrap();
        }

        assert_eq!(index.len().await, 100);

        // Search should return k results
        let query = random_vector(dim);
        let results = index.search(&query, 10).await.unwrap();
        assert_eq!(results.len(), 10);

        // Results should be sorted by distance
        for i in 1..results.len() {
            assert!(results[i].distance >= results[i - 1].distance);
        }
    }

    #[tokio::test]
    async fn test_hnsw_remove() {
        let config = HnswConfig::fast(4);
        let index = HnswIndex::new(config);

        index.insert(1, vec![1.0, 0.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, vec![0.0, 1.0, 0.0, 0.0]).await.unwrap();

        assert_eq!(index.len().await, 2);

        let removed = index.remove(1).await.unwrap();
        assert!(removed);
        assert_eq!(index.len().await, 1);

        let not_found = index.remove(1).await.unwrap();
        assert!(!not_found);
    }

    #[tokio::test]
    async fn test_hnsw_stats() {
        let config = HnswConfig::fast(4);
        let index = HnswIndex::new(config);

        index.insert(1, vec![1.0, 0.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, vec![0.0, 1.0, 0.0, 0.0]).await.unwrap();

        let _ = index.search(&[1.0, 0.0, 0.0, 0.0], 2).await;

        let stats = index.stats();
        assert_eq!(stats.insertions, 2);
        assert_eq!(stats.searches, 1);
        assert!(stats.distance_computations > 0);
    }

    #[test]
    fn test_distance_metrics() {
        let config = HnswConfig::fast(3);
        let index = HnswIndex::new(config);

        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];

        // Cosine distance: identical vectors = 0
        assert!((index.distance(&a, &b) - 0.0).abs() < 0.001);
        // Cosine distance: orthogonal vectors = 1
        assert!((index.distance(&a, &c) - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_config_presets() {
        let fast = HnswConfig::fast(128);
        assert_eq!(fast.m, 12);
        assert_eq!(fast.ef_construction, 100);

        let accurate = HnswConfig::accurate(128);
        assert_eq!(accurate.m, 32);
        assert_eq!(accurate.ef_construction, 400);

        let balanced = HnswConfig::balanced(128);
        assert_eq!(balanced.m, 16);
        assert_eq!(balanced.ef_construction, 200);
    }
}
