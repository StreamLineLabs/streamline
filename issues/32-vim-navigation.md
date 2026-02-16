# Issue 32: Vim-style Navigation

## Summary
Implement vim-style keyboard navigation for power users, enabling j/k navigation through lists, g/G for top/bottom, and / for quick search.

## Current State
- Basic keyboard shortcuts exist (Cmd+K for command palette, Escape for modals)
- No vim-style navigation in lists
- No selection state tracking

## Requirements
1. j/k navigation for lists (topics, consumer groups, messages)
2. g/G for top/bottom of list
3. / for quick search focus
4. Enter to select current item
5. Visual selection indicator

## Implementation Tasks

1. **Add VimNavigator class** (`src/ui/static/main.js`)
   - Track selected index state
   - Handle j/k/g/G/Enter keys
   - Support multiple navigable lists
   - Acceptance: Class functional

2. **Add navigable list styles** (`src/ui/static/main.css`)
   - .vim-navigable class for lists
   - .vim-selected class for current item
   - Visual focus indicator
   - Acceptance: Styles applied

3. **Update topic list template** (`src/ui/templates.rs`)
   - Add vim-navigable class to topic list
   - Add data attributes for navigation
   - Acceptance: Topics navigable

4. **Update consumer groups list** (`src/ui/templates.rs`)
   - Add vim-navigable class to groups list
   - Acceptance: Groups navigable

5. **Update messages list** (`src/ui/templates.rs`)
   - Add vim-navigable class to messages
   - Acceptance: Messages navigable

6. **Add search focus shortcut** (`src/ui/static/main.js`)
   - / key focuses search input
   - n/N for next/prev search result
   - Acceptance: Search shortcuts work

7. **Add vim mode toggle** (`src/ui/static/main.js`)
   - Store preference in localStorage
   - Toggle via keyboard shortcut
   - Acceptance: Mode toggleable

## Acceptance Criteria
- [ ] j/k navigation works in topic list
- [ ] j/k navigation works in consumer groups list
- [ ] j/k navigation works in messages list
- [ ] g/G navigates to top/bottom
- [ ] / focuses search input
- [ ] Enter selects current item
- [ ] Visual indicator shows selected item

## Labels
`enhancement`, `ui`, `ux`, `keyboard-shortcuts`, `priority-medium`
