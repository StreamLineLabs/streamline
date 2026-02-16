# Issue 34: Audit Log Viewer

## Summary
Implement a real-time audit log viewer showing admin operations like topic creation, ACL changes, and configuration updates.

## Current State
- Audit logging exists in `src/audit.rs` (AuditLog struct)
- Logs are written to file and can be queried
- No web UI for viewing audit logs

## Requirements
1. Audit log page showing recent operations
2. Real-time updates via SSE
3. Filtering by operation type, user, resource
4. Search functionality
5. Export to CSV/JSON

## Implementation Tasks

1. **Add audit log API endpoint** (`src/ui/routes.rs`)
   - GET /api/audit - List recent audit events
   - Support pagination and filtering
   - Acceptance: API returns audit events

2. **Add AuditEvent types** (`src/ui/client.rs`)
   - AuditEvent struct with fields: timestamp, user, operation, resource, details
   - AuditEventType enum
   - Acceptance: Types defined

3. **Create audit log page template** (`src/ui/templates.rs`)
   - Table with timestamp, user, operation, resource, details columns
   - Filter controls
   - Search box
   - Acceptance: Page renders

4. **Add audit log route** (`src/ui/routes.rs`)
   - GET /audit - Render audit log page
   - Acceptance: Route works

5. **Add audit log JavaScript** (`src/ui/static/main.js`)
   - Load and display audit events
   - Filtering functionality
   - Search functionality
   - Export functions
   - Acceptance: UI interactive

6. **Add audit log CSS** (`src/ui/static/main.css`)
   - Table styles
   - Operation type badges
   - Filter control styles
   - Acceptance: Styles applied

## Acceptance Criteria
- [ ] Audit log page shows recent operations
- [ ] Filtering by operation type works
- [ ] Filtering by user works
- [ ] Search works
- [ ] Export to CSV works
- [ ] Export to JSON works

## Labels
`enhancement`, `ui`, `security`, `priority-medium`
