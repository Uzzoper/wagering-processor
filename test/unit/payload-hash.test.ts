import { describe, expect, test } from 'bun:test';

import { payloadHash, type PayloadHashInput } from '../../src/application/payload-hash';

function base(): PayloadHashInput {
  return {
    providerId: 'provider-a',
    externalTransactionId: 'tx-1',
    playerId: 'player-1',
    walletId: 'wallet-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: 'BET',
    amount: '25.00',
    currency: 'BRL',
    referenceExternalTransactionId: null,
  };
}

describe('payloadHash', () => {
  test('mesma carga com ordem de campos diferente gera o mesmo hash', () => {
    const reordered: PayloadHashInput = {
      kind: 'BET',
      currency: 'BRL',
      amount: '25.00',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      providerId: 'provider-a',
      externalTransactionId: 'tx-1',
      referenceExternalTransactionId: null,
    };

    expect(payloadHash(base())).toBe(payloadHash(reordered));
  });

  test('referência ausente (undefined) equivale a null', () => {
    const { referenceExternalTransactionId: _ignored, ...withoutRef } = base();

    expect(payloadHash(withoutRef)).toBe(payloadHash(base()));
  });

  test('valor diferente gera hash diferente', () => {
    expect(payloadHash({ ...base(), amount: '30.00' })).not.toBe(payloadHash(base()));
    expect(payloadHash({ ...base(), kind: 'WIN' })).not.toBe(payloadHash(base()));
    expect(payloadHash({ ...base(), referenceExternalTransactionId: 'tx-0' })).not.toBe(
      payloadHash(base()),
    );
  });

  test('formato SHA-256 hex de 64 chars', () => {
    expect(payloadHash(base())).toMatch(/^[0-9a-f]{64}$/);
  });
});
