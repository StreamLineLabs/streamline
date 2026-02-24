import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { StreamlineDataSourceOptions, StreamlineSecureJsonData } from './types';

type Props = DataSourcePluginOptionsEditorProps<StreamlineDataSourceOptions, StreamlineSecureJsonData>;

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  const onUrlChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...jsonData, url: event.target.value },
    });
  };

  const onTimeoutChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...jsonData, timeout: parseInt(event.target.value, 10) || 30 },
    });
  };

  const onApiKeyChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: { ...secureJsonData, apiKey: event.target.value },
    });
  };

  const onApiKeyReset = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, apiKey: false },
      secureJsonData: { ...secureJsonData, apiKey: '' },
    });
  };

  return (
    <>
      <InlineField label="Streamline URL" labelWidth={20} tooltip="HTTP API endpoint (e.g., http://localhost:9094)">
        <Input
          value={jsonData.url || ''}
          placeholder="http://localhost:9094"
          width={40}
          onChange={onUrlChange}
        />
      </InlineField>

      <InlineField label="Timeout (seconds)" labelWidth={20} tooltip="Query timeout">
        <Input
          type="number"
          value={jsonData.timeout || 30}
          width={10}
          onChange={onTimeoutChange}
        />
      </InlineField>

      <InlineField label="API Key" labelWidth={20} tooltip="Optional API key for authentication">
        <SecretInput
          isConfigured={secureJsonFields?.apiKey ?? false}
          value={secureJsonData?.apiKey || ''}
          placeholder="sl_key_..."
          width={40}
          onReset={onApiKeyReset}
          onChange={onApiKeyChange}
        />
      </InlineField>
    </>
  );
}
