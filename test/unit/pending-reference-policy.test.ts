import { describe, expect, test } from 'bun:test';
import {
  PENDING_REF_BACKOFF_BASE_MS,
  PENDING_REF_BACKOFF_CAP_MS,
  pendingRefBackoffMs,
  pendingRefExpired,
} from '../../src/domain/pending-reference-policy';

describe('pendingRefBackoffMs', () => {
  test(' primeira tentativa fica dentro de [15s, 30s]', () => {
    for (let index = 0; index < 50; index += 1) {
      const value = pendingRefBackoffMs(1);
      expect(value).toBeGreaterThanOrEqual(PENDING_REF_BACKOFF_BASE_MS / 2);
      expect(value).toBeLessThanOrEqual(PENDING_REF_BACKOFF_BASE_MS);
    }
  });

  test('tentativas grandes respeitam o teto de 1h (jitter em [30min, 1h])', () => {
    for (let attempts = 8; attempts <= 40; attempts += 1) {
      const value = pendingRefBackoffMs(attempts);
      expect(value).toBeLessThanOrEqual(PENDING_REF_BACKOFF_CAP_MS);
      expect(value).toBeGreaterThanOrEqual(PENDING_REF_BACKOFF_CAP_MS / 2);
    }
  });

  test('crescimento exponencial com jitter em faces conhecidas', () => {
    // attempts=3 → goal = 30s * 4 = 120s; jitter em [60s, 120s].
    for (let index = 0; index < 50; index += 1) {
      const value = pendingRefBackoffMs(3);
      expect(value).toBeGreaterThanOrEqual(60_000);
      expect(value).toBeLessThanOrEqual(120_000);
    }
  });

  test('base/caps customizáveis (injeção de teste)', () => {
    const value = pendingRefBackoffMs(2, 10, 40);
    expect(value).toBeLessThanOrEqual(20); // goal = 20; jitter [10, 20]
    expect(value).toBeGreaterThanOrEqual(10);
  });
});

describe('pendingRefExpired', () => {
  const ttlHours = 24;
  const maxAttempts = 20;
  const pendingSince = new Date('2026-01-01T00:00:00Z');

  test('recentemente pendente, poucas tentativas → false', () => {
    const now = new Date(pendingSince.getTime() + 60_000);
    expect(pendingRefExpired(pendingSince, 3, now, ttlHours, maxAttempts)).toBe(false);
  });

  test('braço TTL: agora depois do TTL → true', () => {
    const now = new Date(pendingSince.getTime() + 24 * 3_600_000);
    expect(pendingRefExpired(pendingSince, 2, now, ttlHours, maxAttempts)).toBe(true);
  });

  test('braço tentativas: retryAttempts >= maxAttempts → true', () => {
    const now = new Date(pendingSince.getTime() + 60_000);
    expect(pendingRefExpired(pendingSince, 20, now, ttlHours, maxAttempts)).toBe(true);
    expect(pendingRefExpired(pendingSince, 21, now, ttlHours, maxAttempts)).toBe(true);
  });

  test('fora do TTL e abaixo do máximo → false', () => {
    const now = new Date(pendingSince.getTime() + 23 * 3_600_000);
    expect(pendingRefExpired(pendingSince, 19, now, ttlHours, maxAttempts)).toBe(false);
  });
});
