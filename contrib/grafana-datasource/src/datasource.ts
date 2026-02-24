import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import { StreamlineQuery, StreamlineDataSourceOptions } from './types';

export class DataSource extends DataSourceApi<StreamlineQuery, StreamlineDataSourceOptions> {
  url: string;

  constructor(instanceSettings: DataSourceInstanceSettings<StreamlineDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url || 'http://localhost:9094';
  }

  async query(options: DataQueryRequest<StreamlineQuery>): Promise<DataQueryResponse> {
    const { range } = options;
    const from = range!.from.toISOString();
    const to = range!.to.toISOString();

    const promises = options.targets
      .filter((t) => !t.hide && t.queryText)
      .map(async (target) => {
        let sql = target.queryText
          .replace(/\$__timeFrom/g, `'${from}'`)
          .replace(/\$__timeTo/g, `'${to}'`)
          .replace(/\$__interval/g, `'${options.intervalMs}ms'`)
          .replace(/\$__timeGroup\((\w+),\s*([^)]+)\)/g, `time_bucket($2, $1)`);

        const response = await getBackendSrv().post(`${this.url}/api/v1/query`, {
          query: sql,
          format: target.format || 'table',
        });

        const frame = new MutableDataFrame({
          refId: target.refId,
          fields: [],
        });

        if (response.columns && response.rows) {
          for (const col of response.columns) {
            const isTime = col.toLowerCase().includes('time') || col.toLowerCase().includes('timestamp');
            frame.addField({
              name: col,
              type: isTime ? FieldType.time : FieldType.number,
            });
          }
          for (const row of response.rows) {
            frame.appendRow(row.map((v: string, i: number) => {
              const col = response.columns[i];
              const isTime = col.toLowerCase().includes('time') || col.toLowerCase().includes('timestamp');
              return isTime ? new Date(v).getTime() : parseFloat(v) || v;
            }));
          }
        }

        return frame;
      });

    const data = await Promise.all(promises);
    return { data };
  }

  async testDatasource(): Promise<{ status: string; message: string }> {
    try {
      const response = await getBackendSrv().get(`${this.url}/health`);
      return { status: 'success', message: `Connected to Streamline at ${this.url}` };
    } catch (err) {
      return { status: 'error', message: `Failed to connect: ${err}` };
    }
  }
}
