# Issue 35: Time Machine Mode

## Summary
Implement a visual timeline scrubber for topic data - the primary market differentiator allowing users to browse topic history through a visual timeline interface.

## Current State
- Topic browse page exists with message listing
- Offset-to-timestamp resolution supported
- No timeline visualization

## Requirements
1. Timeline slider showing topic's time range
2. Visual tick marks for message density
3. Click-to-seek to any point in time
4. Integration with existing browse functionality
5. Playback speed controls
6. Jump to specific timestamp input

## Implementation Tasks

1. **Add timeline API endpoint** (`src/ui/routes.rs`)
   - GET /api/topics/:name/timeline - Returns timeline data
   - Response includes start/end timestamps, message counts per bucket
   - Acceptance: API returns timeline data

2. **Add TimeMachine types** (`src/ui/client.rs`)
   - TimelineData struct with start_ts, end_ts, buckets
   - TimelineBucket struct with timestamp, count, notable_events
   - Acceptance: Types defined

3. **Update topic browse template** (`src/ui/templates.rs`)
   - Add timeline slider component
   - Add playback controls (play/pause, speed)
   - Add timestamp jump input
   - Acceptance: Timeline UI renders

4. **Add time machine JavaScript** (`src/ui/static/main.js`)
   - TimeMachine class for timeline control
   - Load timeline data on topic browse
   - Seek functionality
   - Playback with configurable speed
   - Real-time position updates
   - Acceptance: Timeline interactive

5. **Add time machine CSS** (`src/ui/static/main.css`)
   - Timeline slider styles
   - Density visualization (tick marks)
   - Playback control styles
   - Acceptance: Styles applied

## Acceptance Criteria
- [ ] Timeline slider displays topic time range
- [ ] Message density visualization works
- [ ] Click-to-seek navigates to correct offset
- [ ] Playback controls work
- [ ] Jump to timestamp works
- [ ] Integrates with existing browse

## Labels
`enhancement`, `ui`, `priority-high`, `market-differentiator`
