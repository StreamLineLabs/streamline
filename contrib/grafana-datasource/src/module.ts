import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { StreamlineQuery, StreamlineDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, StreamlineQuery, StreamlineDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
