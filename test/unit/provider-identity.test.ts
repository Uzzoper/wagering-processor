import { describe, expect, test } from 'bun:test';

import { NoopProviderIdentity } from '../../src/infra/auth/noop-provider-identity';
import { InvalidProviderIdentityError } from '../../src/domain/errors';

describe('NoopProviderIdentity', () => {
  const identity = new NoopProviderIdentity();

  test('aceita providerId válido', () => {
    expect(identity.resolve('provider-a')).toBe('provider-a');
  });

  test('rejeita providerId vazio', () => {
    expect(() => identity.resolve('')).toThrow(InvalidProviderIdentityError);
  });

  test('rejeita providerId só com espaços', () => {
    expect(() => identity.resolve('   ')).toThrow(InvalidProviderIdentityError);
  });

  test('rejeita providerId maior que o máximo', () => {
    expect(() => identity.resolve('a'.repeat(129))).toThrow(InvalidProviderIdentityError);
  });
});
