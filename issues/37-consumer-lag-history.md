# Issue 37: Consumer Lag Historical Trends

## Summary
Implement consumer lag historical visualization showing lag trends over time with sparklines, velocity indicators, and alert integration.

## Current State
- UI shows point-in-time lag only
- Backend has lag calculation (`src/consumer/lag.rs`)
- No historical trend storage or visualization

## Requirements
1. Lag history graph (1h, 6h, 24h, 7d views)
2. Per-partition sparklines
3. Lag velocity indicator (growing/stable/shrinking)
4. Alert integration when lag exceeds threshold
5. Export lag data as CSV

## Implementation Tasks

1. **Add lag history types** (`src/ui/client.rs`)
   - LagHistoryPoint struct (timestamp, lag, velocity)
   - LagHistory struct (consumer_group, partition, points)
   - LagVelocity enum (Growing, Stable, Shrinking)
   - Acceptance: Types defined

2. **Add lag history API endpoint** (`src/ui/routes.rs`)
   - GET /api/consumer-groups/:id/lag/history?range=1h
   - Support ranges: 1h, 6h, 24h, 7d
   - Return per-partition history
   - Acceptance: API returns history data

3. **Enhance consumer group details template** (`src/ui/templates.rs`)
   - Add time range selector
   - Add lag history chart container
   - Add per-partition sparklines
   - Add velocity indicators
   - Export button
   - Acceptance: UI elements render

4. **Add lag history JavaScript** (`src/ui/static/main.js`)
   - LagHistoryChart class for visualization
   - Sparkline rendering
   - Time range switching
   - Real-time updates
   - CSV export functionality
   - Acceptance: Charts interactive

5. **Add lag history CSS** (`src/ui/static/main.css`)
   - Chart container styles
   - Sparkline styles
   - Velocity indicator styles
   - Time range selector styles
   - Acceptance: Styles applied

## Acceptance Criteria
- [ ] Lag history graph displays correctly
- [ ] Time range selector works (1h, 6h, 24h, 7d)
- [ ] Per-partition sparklines show trends
- [ ] Velocity indicators show lag direction
- [ ] CSV export works

## Labels
`enhancement`, `ui`, `consumer-groups`, `priority-medium`
