import React from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, Select, TextArea } from '@grafana/ui';
import { DataSource } from './datasource';
import { StreamlineQuery, StreamlineDataSourceOptions, DEFAULT_QUERY } from './types';

type Props = QueryEditorProps<DataSource, StreamlineQuery, StreamlineDataSourceOptions>;

const FORMAT_OPTIONS = [
  { label: 'Time Series', value: 'time_series' as const },
  { label: 'Table', value: 'table' as const },
];

const EXAMPLE_QUERIES = [
  {
    label: '📊 Messages per minute',
    value: `SELECT $__timeGroup(timestamp, $__interval) as time,
  COUNT(*) as messages
FROM events
WHERE timestamp BETWEEN $__timeFrom AND $__timeTo
GROUP BY time ORDER BY time`,
  },
  {
    label: '📈 Consumer group lag',
    value: `SELECT group_id, SUM(lag) as total_lag
FROM consumer_group_offsets
GROUP BY group_id
ORDER BY total_lag DESC LIMIT 20`,
  },
  {
    label: '⚡ Produce latency p99',
    value: `SELECT $__timeGroup(timestamp, $__interval) as time,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) as p99
FROM produce_latency
WHERE timestamp BETWEEN $__timeFrom AND $__timeTo
GROUP BY time ORDER BY time`,
  },
  {
    label: '🔔 Anomaly alerts',
    value: `SELECT timestamp, anomaly_type, severity, resource, description
FROM anomaly_events
ORDER BY timestamp DESC LIMIT 50`,
  },
  {
    label: '📦 Topic throughput',
    value: `SELECT topic, COUNT(*) as msg_count,
  SUM(LENGTH(value)) as total_bytes
FROM messages
WHERE timestamp > NOW() - INTERVAL '5 minutes'
GROUP BY topic ORDER BY msg_count DESC LIMIT 10`,
  },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const currentQuery = query.queryText || DEFAULT_QUERY.queryText || '';
  const currentFormat = query.format || 'time_series';

  const onQueryChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, queryText: event.target.value });
  };

  const onFormatChange = (option: { value?: 'time_series' | 'table' }) => {
    onChange({ ...query, format: option.value || 'time_series' });
    onRunQuery();
  };

  const onExampleSelect = (option: { value?: string }) => {
    if (option.value) {
      onChange({ ...query, queryText: option.value });
      onRunQuery();
    }
  };

  return (
    <>
      <InlineField label="Format" labelWidth={10}>
        <Select
          options={FORMAT_OPTIONS}
          value={currentFormat}
          onChange={onFormatChange}
          width={20}
        />
      </InlineField>

      <InlineField label="Examples" labelWidth={10}>
        <Select
          options={EXAMPLE_QUERIES}
          placeholder="Load example query..."
          onChange={onExampleSelect}
          width={30}
          isClearable
        />
      </InlineField>

      <InlineField label="StreamQL" labelWidth={10} grow>
        <TextArea
          value={currentQuery}
          onChange={onQueryChange}
          onBlur={onRunQuery}
          rows={8}
          placeholder="SELECT * FROM events LIMIT 10"
          style={{ fontFamily: 'ui-monospace, monospace', fontSize: '13px' }}
        />
      </InlineField>

      <div style={{ fontSize: '11px', color: '#8e8e8e', marginTop: 4, marginLeft: 80 }}>
        Macros: <code>$__timeFrom</code> <code>$__timeTo</code> <code>$__interval</code> <code>$__timeGroup(col, interval)</code>
        {' | '}Ctrl+Enter to run
      </div>
    </>
  );
}
