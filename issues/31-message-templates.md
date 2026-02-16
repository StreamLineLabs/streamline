# Issue 31: Message Production Templates

## Summary
Implement a template system for message production that allows users to save and reuse message formats, including JSON schemas, sample data, and custom generators.

## Current State
- Basic message production exists in `src/ui/templates.rs` (`topic_produce` method)
- Messages are produced via `/api/topics/:name/messages` endpoint
- No template storage or reuse mechanism

## Requirements
1. Template storage in localStorage
2. Template CRUD operations (Create, Read, Update, Delete)
3. Template variables with substitution
4. JSON schema validation
5. Template import/export

## Implementation Tasks

1. **Add Template types to client.rs** (`src/ui/client.rs`)
   - Add `MessageTemplate` struct with fields: id, name, topic, key_template, value_template, headers
   - Acceptance: Types defined and serializable

2. **Create Template Manager JavaScript class** (`src/ui/static/main.js`)
   - CRUD operations for templates in localStorage
   - Template variable substitution (e.g., `{{timestamp}}`, `{{uuid}}`, `{{random:1-100}}`)
   - Acceptance: Templates can be saved and loaded

3. **Update topic produce page** (`src/ui/templates.rs`)
   - Add template selector dropdown
   - Add "Save as Template" button
   - Show template variables
   - Acceptance: UI shows template options

4. **Add template modal** (`src/ui/templates.rs`)
   - Modal for creating/editing templates
   - Variable documentation/help
   - Acceptance: Modal functional

5. **Add template variable expansion** (`src/ui/static/main.js`)
   - Built-in variables: `{{timestamp}}`, `{{uuid}}`, `{{random:min-max}}`, `{{now:format}}`
   - Custom variable definitions
   - Acceptance: Variables expand correctly

6. **Add import/export functionality** (`src/ui/static/main.js`)
   - Export templates to JSON file
   - Import templates from JSON file
   - Acceptance: Import/export works

## Acceptance Criteria
- [ ] Templates can be saved and loaded
- [ ] Template variables are substituted on produce
- [ ] Templates are persisted in localStorage
- [ ] Import/export functionality works
- [ ] UI integrates with existing produce page

## Labels
`enhancement`, `ui`, `ux`, `priority-medium`
