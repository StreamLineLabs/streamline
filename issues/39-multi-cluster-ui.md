# Issue #39: Multi-Cluster Management UI

## Summary
Add multi-cluster management capabilities to the Streamline web UI, allowing users to manage multiple Streamline clusters from a single interface with cluster switching, cross-cluster comparison, and unified search.

## Priority
Medium - Phase 3 Enterprise Feature

## Requirements

### 1. Cluster Selector in Header
- Dropdown in header to switch between clusters
- Show current cluster name and health status
- Quick access to cluster management
- Persist selected cluster in localStorage

### 2. Cluster Management Page (`/clusters`)
- List all registered clusters with health status
- Add/edit/remove cluster connections
- Show cluster details: version, broker count, topic count
- Connection status indicators
- Test connection functionality

### 3. Cross-Cluster Health Comparison
- Side-by-side cluster health metrics
- Unified dashboard view across clusters
- Alert count per cluster
- Replication status if mirroring enabled

### 4. Unified Topic Search
- Search topics across all clusters
- Show cluster name in search results
- Navigate directly to topic on any cluster
- Command palette integration

### 5. Per-Cluster Configuration
- Store cluster connection details
- Cluster nickname/alias support
- Color coding for cluster identification
- Authentication settings per cluster

### 6. API Endpoints
- `GET /api/clusters` - List registered clusters
- `POST /api/clusters` - Add cluster
- `PUT /api/clusters/:id` - Update cluster
- `DELETE /api/clusters/:id` - Remove cluster
- `POST /api/clusters/:id/test` - Test connection
- `GET /api/clusters/:id/health` - Get cluster health

## Technical Implementation

### Types (client.rs)
```rust
pub struct ClusterInfo {
    pub id: String,
    pub name: String,
    pub url: String,
    pub health: ClusterHealth,
    pub version: Option<String>,
    pub broker_count: i32,
    pub topic_count: i32,
    pub added_at: String,
    pub last_seen: Option<String>,
}

pub enum ClusterHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

pub struct ClusterConfig {
    pub name: String,
    pub url: String,
    pub color: Option<String>,
    pub auth: Option<ClusterAuth>,
}

pub struct ClusterAuth {
    pub auth_type: String,
    pub username: Option<String>,
    pub password: Option<String>,
}
```

### Routes (routes.rs)
- Add page handlers for `/clusters`
- Add API handlers for cluster management

### Templates (templates.rs)
- `clusters_page()` - Cluster management page
- Update header with cluster selector

### JavaScript (main.js)
- Cluster selector functionality
- Cluster management forms
- Cross-cluster search integration

### CSS (main.css)
- Cluster selector styles
- Health indicators
- Cluster cards

## Acceptance Criteria
- [ ] Users can add/edit/remove cluster connections
- [ ] Cluster selector appears in header with current cluster
- [ ] Switching clusters updates all views
- [ ] Health status is visible for all clusters
- [ ] Search works across multiple clusters
- [ ] Cluster colors help distinguish views

## Files to Modify
- `src/ui/client.rs` - Add cluster types
- `src/ui/routes.rs` - Add cluster API routes
- `src/ui/templates.rs` - Add cluster templates and header update
- `src/ui/static/main.js` - Add cluster management JS
- `src/ui/static/main.css` - Add cluster styles
