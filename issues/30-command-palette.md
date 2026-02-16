# Issue 30: Command Palette (Cmd+K)

## Summary
Implement a command palette similar to VS Code/Spotlight that provides global fuzzy search across all entities and actions. This is the single most impactful UX improvement for the Web UI.

## Current State
- Basic keyboard shortcuts exist in `src/ui/static/main.js:112-126`
- Ctrl+K currently only focuses the search input
- No global entity search or action execution

## Requirements
1. Global `Cmd+K` / `Ctrl+K` trigger opens palette modal
2. Fuzzy search across:
   - Topics (by name, regex)
   - Consumer Groups (by name, state)
   - Brokers (by ID)
   - Recent actions
3. Action execution from palette:
   - Navigate to entity
   - Create topic
   - Produce message
   - Reset offsets
   - View metrics
4. Recent commands shown at top
5. Contextual actions based on current page

## Implementation Tasks

1. **Add search API endpoint** (`src/ui/routes.rs`)
   - Add `GET /api/search?q={query}&type={entity_type}` route
   - Return `{ results: [{ type, name, url, description }] }`
   - Acceptance: Endpoint returns fuzzy-matched results

2. **Add search client method** (`src/ui/client.rs`)
   - Add `search(&self, query: &str, entity_type: Option<&str>)` method
   - Acceptance: Client can call search API

3. **Create CommandPalette JavaScript class** (`src/ui/static/main.js`)
   - Modal component with input and results list
   - Fuzzy search with debouncing
   - Keyboard navigation (up/down/enter/escape)
   - Acceptance: Palette opens with Cmd+K, shows results

4. **Add palette HTML/CSS** (`src/ui/templates.rs`, `src/ui/static/main.css`)
   - Modal overlay with search input
   - Results list with icons
   - Loading state
   - Acceptance: Styled modal matching theme

5. **Add action handlers** (`src/ui/static/main.js`)
   - Navigation actions
   - Create topic modal trigger
   - Produce message modal trigger
   - Acceptance: All actions work from palette

6. **Add recent commands storage** (`src/ui/static/main.js`)
   - Store last 10 commands in localStorage
   - Show recent at top when palette opens
   - Acceptance: Recent commands persist

7. **Add contextual actions** (`src/ui/static/main.js`)
   - Show page-specific actions
   - Topic page: browse, produce, delete
   - Consumer group page: reset offsets
   - Acceptance: Actions change by page

8. **Add tests** (`src/ui/routes.rs`)
   - Unit tests for search endpoint
   - Acceptance: Tests pass

## Acceptance Criteria
- [ ] Cmd+K opens command palette globally
- [ ] Fuzzy search returns topics, groups, brokers
- [ ] Navigation to any entity works
- [ ] Actions execute correctly
- [ ] Recent commands shown at top
- [ ] Keyboard navigation works (arrows, enter, escape)
- [ ] Styled consistently with theme

## Labels
`enhancement`, `ui`, `ux`, `priority-high`
