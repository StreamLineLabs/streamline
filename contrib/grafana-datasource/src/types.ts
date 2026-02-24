import { DataSourceJsonData, DataQuery } from '@grafana/data';

export interface StreamlineQuery extends DataQuery {
  queryText: string;
  format: 'time_series' | 'table';
  maxDataPoints?: number;
}

export const DEFAULT_QUERY: Partial<StreamlineQuery> = {
  queryText: `SELECT
  $__timeGroup(timestamp, $__interval) as time,
  COUNT(*) as event_count
FROM events
WHERE timestamp BETWEEN $__timeFrom AND $__timeTo
GROUP BY time
ORDER BY time`,
  format: 'time_series',
};

export interface StreamlineDataSourceOptions extends DataSourceJsonData {
  url: string;
  defaultDatabase?: string;
  timeout?: number;
}

export interface StreamlineSecureJsonData {
  apiKey?: string;
}
