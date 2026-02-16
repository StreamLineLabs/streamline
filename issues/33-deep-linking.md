# Issue 33: Deep Linking Enhancement

## Summary
Enhance deep linking to support sharing specific messages, offsets, and states. URLs should be copy-paste shareable and restore full UI state.

## Current State
- Basic routing exists (topics, consumer groups, brokers)
- Message browser has basic partition/offset parameters
- No URL hash support for UI state
- No share button or link copying

## Requirements
1. Support hash-based state encoding
2. Share buttons on key pages
3. Copy to clipboard functionality
4. Support for message-specific links
5. URL state restoration on load

## Implementation Tasks

1. **Create DeepLink utility class** (`src/ui/static/main.js`)
   - Encode/decode URL state to hash
   - Handle state changes
   - Support multiple parameters
   - Acceptance: URLs properly encode state

2. **Add share button component** (`src/ui/static/main.js`)
   - Share button UI
   - Copy to clipboard
   - Show toast on copy
   - Acceptance: Share button works

3. **Update message browser** (`src/ui/templates.rs`)
   - Add share button per message
   - Link format: `/topics/{topic}/browse?partition={p}&offset={offset}#message={offset}`
   - Acceptance: Messages shareable

4. **Add state restoration** (`src/ui/static/main.js`)
   - Parse URL on page load
   - Scroll to highlighted message
   - Restore filter/search state
   - Acceptance: Links restore state

5. **Add deep link CSS** (`src/ui/static/main.css`)
   - Highlighted message style
   - Share button styles
   - Acceptance: Styles applied

## Acceptance Criteria
- [ ] URLs encode current view state
- [ ] Share button copies URL to clipboard
- [ ] Messages can be directly linked
- [ ] Links restore correct UI state
- [ ] Highlighted message is visible

## Labels
`enhancement`, `ui`, `ux`, `priority-medium`
