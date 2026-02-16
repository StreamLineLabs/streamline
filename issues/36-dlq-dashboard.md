# Issue 36: DLQ Dashboard Enhancement

## Summary
Implement a dedicated Dead Letter Queue (DLQ) dashboard for monitoring and managing failed messages with error categorization, retry workflows, and correlation to source topics.

## Current State
- DLQ module exists (`src/dlq/mod.rs`)
- Basic DLQ tracking available
- No dedicated UI for DLQ management

## Requirements
1. Dedicated `/dlq` page showing all DLQ topics
2. Error categorization:
   - Deserialization failures
   - Schema mismatches
   - Processing errors
   - Timeout errors
3. One-click retry workflows
4. Spike detection visualization
5. Original message metadata display
6. Correlation with source topic

## Implementation Tasks

1. **Add DLQ types** (`src/ui/client.rs`)
   - DlqTopic struct (name, message_count, error_types, source_topic)
   - DlqMessage struct (offset, error_type, original_topic, timestamp, error_details)
   - DlqStats struct (total_messages, by_error_type, by_source)
   - Acceptance: Types defined

2. **Add DLQ page route** (`src/ui/routes.rs`)
   - GET /dlq - DLQ dashboard page
   - GET /dlq/:topic - Specific DLQ topic details
   - Acceptance: Routes work

3. **Add DLQ API endpoints** (`src/ui/routes.rs`)
   - GET /api/dlq - List DLQ topics with stats
   - GET /api/dlq/:topic/messages - Get messages in DLQ
   - POST /api/dlq/:topic/retry - Retry messages
   - POST /api/dlq/:topic/purge - Purge messages
   - Acceptance: APIs functional

4. **Create DLQ dashboard template** (`src/ui/templates.rs`)
   - DLQ topics overview with message counts
   - Error type distribution chart
   - Spike detection visualization
   - Message browser with error details
   - Retry/purge action buttons
   - Acceptance: Page renders

5. **Add DLQ JavaScript** (`src/ui/static/main.js`)
   - Load and display DLQ topics
   - Error type filtering
   - Retry/purge functionality
   - Spike visualization
   - Acceptance: UI interactive

6. **Add DLQ CSS** (`src/ui/static/main.css`)
   - DLQ topic cards
   - Error type badges
   - Spike chart styles
   - Action button styles
   - Acceptance: Styles applied

## Acceptance Criteria
- [ ] DLQ dashboard shows all DLQ topics
- [ ] Error categorization displays correctly
- [ ] One-click retry works
- [ ] One-click purge works
- [ ] Spike detection shows visually
- [ ] Original message metadata displayed
- [ ] Source topic correlation shown

## Labels
`enhancement`, `ui`, `dlq`, `priority-high`
