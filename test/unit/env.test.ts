import { describe, expect, test } from 'bun:test';

import { loadConfig } from '../../src/infra/config/env';

describe('loadConfig — retry SQS', () => {
  test('usa defaults de 30s e 1h', () => {
    const config = loadConfig({ WORKERS_ENABLED: 'api' });

    expect(config.sqs.retryBaseDelaySeconds).toBe(30);
    expect(config.sqs.retryMaxDelaySeconds).toBe(3_600);
  });

  test('lê base e teto por env para permitir retries rápidos em teste', () => {
    const config = loadConfig({
      WORKERS_ENABLED: 'api',
      SQS_RETRY_BASE_DELAY_SECONDS: '2',
      SQS_RETRY_MAX_DELAY_SECONDS: '8',
    });

    expect(config.sqs.retryBaseDelaySeconds).toBe(2);
    expect(config.sqs.retryMaxDelaySeconds).toBe(8);
  });

  test('rejeita teto menor que a base ou acima do limite da SQS', () => {
    expect(() => loadConfig({ SQS_RETRY_BASE_DELAY_SECONDS: '10', SQS_RETRY_MAX_DELAY_SECONDS: '9' })).toThrow(
      'SQS_RETRY_MAX_DELAY_SECONDS',
    );
    expect(() => loadConfig({ SQS_RETRY_MAX_DELAY_SECONDS: '43201' })).toThrow(
      'SQS_RETRY_MAX_DELAY_SECONDS',
    );
  });
});
