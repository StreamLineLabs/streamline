# Issue #38: Schema Evolution Timeline UI

## Summary
Add a schema evolution timeline UI to the Streamline web interface that visualizes schema version history, enables version comparison with side-by-side diffs, and shows compatibility status for each schema subject.

## Priority
Medium - Phase 2 Core Differentiator

## Requirements

### 1. Schema Registry Browser (`/schemas`)
- List all registered schema subjects
- Show subject type (Avro, Protobuf, JSON Schema)
- Display latest version number and compatibility level
- Search/filter by subject name
- Quick view of linked topics

### 2. Schema Evolution Timeline (`/schemas/:subject`)
- Visual timeline showing all versions of a schema
- Version cards showing:
  - Version number and timestamp
  - Compatibility status badge (BACKWARD, FORWARD, FULL, NONE)
  - "What changed" summary (fields added/removed/modified)
  - Link to diff with previous version
- Click to view full schema definition
- Linked topics list

### 3. Schema Diff View (`/schemas/:subject/diff/:v1/:v2`)
- Side-by-side schema comparison
- Field-level change highlighting:
  - Green: Added fields
  - Red: Removed fields
  - Yellow: Modified fields
- Syntax highlighting for schema definitions
- Copy buttons for each version

### 4. API Endpoints
- `GET /api/schemas` - List all subjects
- `GET /api/schemas/:subject` - Get subject details with version history
- `GET /api/schemas/:subject/versions/:version` - Get specific version
- `GET /api/schemas/:subject/diff/:v1/:v2` - Get diff between versions

## Technical Implementation

### Types (client.rs)
```rust
pub enum SchemaType {
    Avro,
    Protobuf,
    JsonSchema,
}

pub enum CompatibilityLevel {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

pub struct SchemaSubject {
    pub name: String,
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub compatibility: CompatibilityLevel,
    pub linked_topics: Vec<String>,
    pub versions_count: i32,
}

pub struct SchemaVersion {
    pub version: i32,
    pub schema_id: i32,
    pub schema: String,
    pub registered_at: String,
    pub compatibility_status: String,
    pub changes_summary: Vec<SchemaChange>,
}

pub struct SchemaChange {
    pub change_type: String,  // "added", "removed", "modified"
    pub field_name: String,
    pub description: String,
}

pub struct SchemaDiff {
    pub subject: String,
    pub version1: i32,
    pub version2: i32,
    pub schema1: String,
    pub schema2: String,
    pub changes: Vec<SchemaChange>,
}
```

### Routes (routes.rs)
- Add route handlers for all API endpoints
- Add page handlers for `/schemas`, `/schemas/:subject`, `/schemas/:subject/diff/:v1/:v2`

### Templates (templates.rs)
- `schema_list()` - Schema registry browser
- `schema_details()` - Schema evolution timeline
- `schema_diff()` - Side-by-side diff view

### JavaScript (main.js)
- `SchemaEvolutionTimeline` class for interactive timeline
- Syntax highlighting for schema definitions
- Copy-to-clipboard functionality

### CSS (main.css)
- Timeline visualization styles
- Diff view with highlighting
- Compatibility badge colors

## Acceptance Criteria
- [ ] Schema subjects are listed with type and compatibility info
- [ ] Evolution timeline shows all versions with visual representation
- [ ] Diff view highlights field-level changes
- [ ] Compatibility badges show appropriate status
- [ ] Topics linked to each schema are displayed
- [ ] UI is responsive and matches existing design

## Files to Modify
- `src/ui/client.rs` - Add schema types
- `src/ui/routes.rs` - Add API and page routes
- `src/ui/templates.rs` - Add template functions
- `src/ui/static/main.js` - Add SchemaEvolutionTimeline class
- `src/ui/static/main.css` - Add schema UI styles
