import { describe, expect, test } from 'bun:test';

import {
  AmountMismatchError,
  InvalidReferenceError,
  InvalidTransactionStateError,
  LedgerNotApplicableError,
  ReversalWouldOverdrawError,
} from '../../src/domain/errors';
import { FailureCodes } from '../../src/domain/failure-code';
import { Money } from '../../src/domain/money';
import {
  WagerTransaction,
  type CreateWagerTransactionProps,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wager-transaction';
import { LedgerDirection } from '../../src/domain/wallet-ledger-entry';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
const usd = (amount: string): Money => Money.from({ amount, currency: 'USD' });

const CREATED_AT = new Date('2026-01-01T00:00:00.000Z');
const PROCESSED_AT = new Date('2026-01-02T00:00:00.000Z');

function makeTx(overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  return WagerTransaction.create({
    id: 'tx-1',
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    idempotencyKey: 'provider-a:ext-1',
    payloadHash: 'hash-1',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: brl('25.00'),
    createdAt: CREATED_AT,
    ...overrides,
  });
}

function processed(overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  const transaction = makeTx(overrides);
  transaction.markProcessed(undefined, PROCESSED_AT);
  return transaction;
}

describe('WagerTransaction.create', () => {
  test('nasce em PENDING, sem referência resolvida nem failureCode', () => {
    const transaction = makeTx({ kind: WagerTransactionKind.Bet });

    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    expect(transaction.referenceTransactionId).toBeUndefined();
    expect(transaction.failureCode).toBeUndefined();
    expect(transaction.processedAt).toBeUndefined();
  });

  test('REFUND exige referenceExternalTransactionId', () => {
    expect(() => makeTx({ kind: WagerTransactionKind.Refund })).toThrow(InvalidReferenceError);
  });

  test('ROLLBACK exige referenceExternalTransactionId', () => {
    expect(() => makeTx({ kind: WagerTransactionKind.Rollback })).toThrow(InvalidReferenceError);
  });

  test('REFUND com referência é aceito e a guarda', () => {
    const transaction = makeTx({
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'bet-ext-1',
    });

    expect(transaction.referenceExternalTransactionId).toBe('bet-ext-1');
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });

  test('referência vazia/em branco é tratada como ausente', () => {
    expect(() =>
      makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: '   ' }),
    ).toThrow(InvalidReferenceError);
  });

  test('WIN pode referenciar uma BET', () => {
    const transaction = makeTx({
      kind: WagerTransactionKind.Win,
      referenceExternalTransactionId: 'bet-ext-1',
    });

    expect(transaction.referenceExternalTransactionId).toBe('bet-ext-1');
  });

  test('OPENING é aceito pelo domínio (validação externa fica na borda)', () => {
    const transaction = makeTx({ kind: WagerTransactionKind.Opening, money: brl('100.00') });

    expect(transaction.kind).toBe(WagerTransactionKind.Opening);
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });
});

describe('WagerTransaction.requiresReference / affectsBalance', () => {
  const cases: Array<[WagerTransactionKind, boolean, boolean]> = [
    [WagerTransactionKind.Opening, false, true],
    [WagerTransactionKind.Bet, false, true],
    [WagerTransactionKind.Win, false, true],
    [WagerTransactionKind.Loss, false, false],
    [WagerTransactionKind.Refund, true, true],
    [WagerTransactionKind.Rollback, true, true],
  ];

  for (const [kind, requiresReference, affectsBalance] of cases) {
    test(`${kind}: requiresReference=${requiresReference}, affectsBalance=${affectsBalance}`, () => {
      const transaction = makeTx({
        kind,
        referenceExternalTransactionId: requiresReference ? 'ref-1' : undefined,
      });

      expect(transaction.requiresReference()).toBe(requiresReference);
      expect(transaction.affectsBalance()).toBe(affectsBalance);
    });
  }
});

describe('WagerTransaction — transições', () => {
  test('markProcessed grava status, referência interna e processedAt', () => {
    const transaction = makeTx({ kind: WagerTransactionKind.Win });

    transaction.markProcessed('bet-internal-id', PROCESSED_AT);

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.referenceTransactionId).toBe('bet-internal-id');
    expect(transaction.processedAt).toBe(PROCESSED_AT);
    expect(transaction.isTerminal()).toBe(true);
  });

  test('markPendingReference a partir de PENDING', () => {
    const transaction = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'r' });

    transaction.markPendingReference();

    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
    expect(transaction.isTerminal()).toBe(false);
  });

  test('PENDING_REFERENCE pode ser processada depois', () => {
    const transaction = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'r' });

    transaction.markPendingReference();
    transaction.markProcessed('ref-internal', PROCESSED_AT);

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
  });

  test('reject grava REJECTED e failureCode', () => {
    const transaction = makeTx();

    transaction.reject(FailureCodes.InsufficientBalance);

    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe(FailureCodes.InsufficientBalance);
    expect(transaction.isTerminal()).toBe(true);
  });

  test('fail grava FAILED e failureCode', () => {
    const transaction = makeTx();

    transaction.fail(FailureCodes.InfraTransient);

    expect(transaction.status).toBe(WagerTransactionStatus.Failed);
    expect(transaction.failureCode).toBe(FailureCodes.InfraTransient);
    expect(transaction.isTerminal()).toBe(true);
  });
});

describe('WagerTransaction — estados terminais são imutáveis', () => {
  const terminals: Array<[string, (tx: WagerTransaction) => void]> = [
    ['PROCESSED', (tx) => tx.markProcessed('ref', PROCESSED_AT)],
    ['REJECTED', (tx) => tx.reject(FailureCodes.InvalidPayload)],
    ['FAILED', (tx) => tx.fail(FailureCodes.InfraTransient)],
  ];

  for (const [label, transition] of terminals) {
    test(`${label}: markProcessed/markPendingReference/reject/fail lançam`, () => {
      const transaction = makeTx();
      transition(transaction);
      expect(transaction.isTerminal()).toBe(true);

      expect(() => transaction.markProcessed('other', PROCESSED_AT)).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => transaction.markPendingReference()).toThrow(InvalidTransactionStateError);
      expect(() => transaction.reject(FailureCodes.InvalidPayload)).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => transaction.fail(FailureCodes.InfraTransient)).toThrow(
        InvalidTransactionStateError,
      );
    });
  }

  test('o erro carrega status atual e transição tentada', () => {
    const transaction = makeTx();
    transaction.reject(FailureCodes.InvalidPayload);

    try {
      transaction.markProcessed(undefined, PROCESSED_AT);
      expect.unreachable('deveria ter lançado InvalidTransactionStateError');
    } catch (error) {
      expect(error).toBeInstanceOf(InvalidTransactionStateError);
      const invalid = error as InvalidTransactionStateError;
      expect(invalid.currentStatus).toBe(WagerTransactionStatus.Rejected);
      expect(invalid.attemptedTransition).toBe('markProcessed');
    }
  });
});

describe('WagerTransaction.matchesPayload', () => {
  test('compara o payloadHash', () => {
    const transaction = makeTx({ payloadHash: 'hash-abc' });

    expect(transaction.matchesPayload('hash-abc')).toBe(true);
    expect(transaction.matchesPayload('hash-xyz')).toBe(false);
  });
});

describe('WagerTransaction.ledgerDirectionFor', () => {
  test('BET debita; WIN/REFUND/OPENING creditam', () => {
    expect(makeTx({ kind: WagerTransactionKind.Bet }).ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(makeTx({ kind: WagerTransactionKind.Win }).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(
      makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'r' }).ledgerDirectionFor(),
    ).toBe(LedgerDirection.Credit);
    expect(makeTx({ kind: WagerTransactionKind.Opening }).ledgerDirectionFor()).toBe(
      LedgerDirection.Credit,
    );
  });

  test('LOSS não gera lançamento', () => {
    expect(() => makeTx({ kind: WagerTransactionKind.Loss }).ledgerDirectionFor()).toThrow(
      LedgerNotApplicableError,
    );
  });

  test('ROLLBACK inverte a direção da referência', () => {
    const rollbackOfBet = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'r' });
    const rollbackOfWin = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'r' });
    const rollbackOfRefund = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'r' });

    expect(rollbackOfBet.ledgerDirectionFor(processed({ kind: WagerTransactionKind.Bet }))).toBe(
      LedgerDirection.Credit,
    );
    expect(rollbackOfWin.ledgerDirectionFor(processed({ kind: WagerTransactionKind.Win }))).toBe(
      LedgerDirection.Debit,
    );
    expect(rollbackOfRefund.ledgerDirectionFor(processed({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'r' }))).toBe(
      LedgerDirection.Debit,
    );
  });

  test('ROLLBACK sem referência lança', () => {
    const rollback = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'r' });

    expect(() => rollback.ledgerDirectionFor()).toThrow(InvalidReferenceError);
  });
});

describe('WagerTransaction.ensureValidReversal — casos válidos', () => {
  test('REFUND de uma BET PROCESSED do mesmo dono e valor', () => {
    const reference = processed({ kind: WagerTransactionKind.Bet, money: brl('25.00') });
    const refund = makeTx({
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'ext-1',
      money: brl('25.00'),
    });

    expect(() => refund.ensureValidReversal(reference, brl('100.00'))).not.toThrow();
  });

  test('ROLLBACK de BET/WIN/REFUND PROCESSED', () => {
    const cases: WagerTransactionKind[] = [
      WagerTransactionKind.Bet,
      WagerTransactionKind.Win,
      WagerTransactionKind.Refund,
    ];

    for (const referenceKind of cases) {
      const reference = processed({
        kind: referenceKind,
        referenceExternalTransactionId: referenceKind === WagerTransactionKind.Refund ? 'r' : undefined,
      });
      const rollback = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'ext-1' });

      expect(() => rollback.ensureValidReversal(reference, brl('1000.00'))).not.toThrow();
    }
  });

  test('REFUND de BET não sofre overdraw mesmo com saldo zero (é crédito)', () => {
    const reference = processed({ kind: WagerTransactionKind.Bet, money: brl('25.00') });
    const refund = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'ext-1' });

    expect(() => refund.ensureValidReversal(reference, brl('0.00'))).not.toThrow();
  });
});

describe('WagerTransaction.ensureValidReversal — casos inválidos', () => {
  test('operação que não é reversão é rejeitada', () => {
    const bet = makeTx({ kind: WagerTransactionKind.Bet });
    const reference = processed({ kind: WagerTransactionKind.Bet });

    expect(() => bet.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('REFUND só referencia BET', () => {
    const reference = processed({ kind: WagerTransactionKind.Win });
    const refund = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'ext-1' });

    expect(() => refund.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('ROLLBACK não referencia ROLLBACK', () => {
    const reference = processed({
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'r',
    });
    const rollback = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'ext-1' });

    expect(() => rollback.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('referência REJECTED é inválida', () => {
    const reference = makeTx({ kind: WagerTransactionKind.Bet });
    reference.reject(FailureCodes.InsufficientBalance);
    const refund = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'ext-1' });

    expect(() => refund.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('referência PENDING_REFERENCE é inválida como referência', () => {
    const reference = makeTx({
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'r',
    });
    reference.markPendingReference();
    const rollback = makeTx({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'ext-1' });

    expect(() => rollback.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('identidade divergente (provider/player/wallet/round) é inválida', () => {
    const refund = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'ext-1' });

    const mismatches: Array<Partial<CreateWagerTransactionProps>> = [
      { providerId: 'provider-b' },
      { playerId: 'player-2' },
      { walletId: 'wallet-2' },
      { roundId: 'round-2' },
    ];

    for (const mismatch of mismatches) {
      const reference = processed({ kind: WagerTransactionKind.Bet, ...mismatch });
      expect(() => refund.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
    }
  });

  test('moeda da referência divergente é inválida', () => {
    const reference = processed({ kind: WagerTransactionKind.Bet, money: usd('25.00') });
    const refund = makeTx({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'ext-1' });

    expect(() => refund.ensureValidReversal(reference, brl('100.00'))).toThrow(InvalidReferenceError);
  });

  test('valor diferente da referência é AmountMismatchError', () => {
    const reference = processed({ kind: WagerTransactionKind.Bet, money: brl('25.00') });
    const refund = makeTx({
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'ext-1',
      money: brl('20.00'),
    });

    expect(() => refund.ensureValidReversal(reference, brl('100.00'))).toThrow(AmountMismatchError);
  });

  test('ROLLBACK que negativaria o saldo é ReversalWouldOverdrawError', () => {
    // ROLLBACK de um WIN (crédito) = débito; saldo atual menor que o valor.
    const reference = processed({ kind: WagerTransactionKind.Win, money: brl('80.00') });
    const rollback = makeTx({
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-1',
      money: brl('80.00'),
    });

    expect(() => rollback.ensureValidReversal(reference, brl('50.00'))).toThrow(
      ReversalWouldOverdrawError,
    );
  });

  test('ROLLBACK com saldo suficiente não sofre overdraw', () => {
    const reference = processed({ kind: WagerTransactionKind.Win, money: brl('80.00') });
    const rollback = makeTx({
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-1',
      money: brl('80.00'),
    });

    expect(() => rollback.ensureValidReversal(reference, brl('100.00'))).not.toThrow();
  });
});

describe('WagerTransaction.rehydrate', () => {
  test('reconstrói estado terminal persistido sem revalidar', () => {
    const transaction = WagerTransaction.rehydrate({
      id: 'tx-9',
      providerId: 'provider-a',
      externalTransactionId: 'ext-9',
      idempotencyKey: 'key-9',
      payloadHash: 'hash-9',
      walletId: 'wallet-9',
      playerId: 'player-9',
      roundId: 'round-9',
      gameId: 'game-9',
      kind: WagerTransactionKind.Refund,
      money: brl('25.00'),
      referenceExternalTransactionId: 'bet-ext-9',
      createdAt: CREATED_AT,
      status: WagerTransactionStatus.Processed,
      referenceTransactionId: 'bet-internal-9',
      processedAt: PROCESSED_AT,
    });

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.referenceTransactionId).toBe('bet-internal-9');
    expect(transaction.processedAt).toBe(PROCESSED_AT);
  });

  test('não revalida a exigência de referência (estado já persistido)', () => {
    const transaction = WagerTransaction.rehydrate({
      id: 'tx-10',
      providerId: 'provider-a',
      externalTransactionId: 'ext-10',
      idempotencyKey: 'key-10',
      payloadHash: 'hash-10',
      walletId: 'wallet-10',
      playerId: 'player-10',
      roundId: 'round-10',
      gameId: 'game-10',
      kind: WagerTransactionKind.Refund,
      money: brl('10.00'),
      createdAt: CREATED_AT,
      status: WagerTransactionStatus.Pending,
    });

    expect(transaction.referenceExternalTransactionId).toBeUndefined();
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });
});
