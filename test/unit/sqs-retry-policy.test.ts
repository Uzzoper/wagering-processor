import { describe, expect, test } from 'bun:test';

import { sqsRetryDelaySeconds } from '../../src/infra/sqs/sqs-retry-policy';

describe('sqsRetryDelaySeconds', () => {
  test('cresce exponencialmente e para no teto', () => {
    expect([1, 2, 3, 4, 8, 100].map((count) => sqsRetryDelaySeconds(count))).toEqual([
      30, 60, 120, 240, 3_600, 3_600,
    ]);
  });

  test('aceita base e teto customizados para testes rápidos', () => {
    expect([1, 2, 3, 4, 10].map((count) => sqsRetryDelaySeconds(count, 2, 8))).toEqual([
      2, 4, 8, 8, 8,
    ]);
  });

  test('rejeita parâmetros fora dos limites', () => {
    expect(() => sqsRetryDelaySeconds(0)).toThrow(RangeError);
    expect(() => sqsRetryDelaySeconds(1, 30, 10)).toThrow(RangeError);
    expect(() => sqsRetryDelaySeconds(1, 30, 43_201)).toThrow(RangeError);
  });
});
