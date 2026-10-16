import { describe, expect, test } from 'bun:test';

import {
  CurrencyMismatchError,
  InvalidMoneyError,
  UnbalancedLedgerEntryError,
} from '../../src/domain/errors';
import { Money } from '../../src/domain/money';
import {
  LedgerDirection,
  WalletLedgerEntry,
  type CreateLedgerEntryProps,
} from '../../src/domain/wallet-ledger-entry';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
const usd = (amount: string): Money => Money.from({ amount, currency: 'USD' });

const CREATED_AT = new Date('2026-02-01T00:00:00.000Z');

function makeEntry(overrides: Partial<CreateLedgerEntryProps> = {}): WalletLedgerEntry {
  return WalletLedgerEntry.create({
    id: 'ledger-1',
    walletId: 'wallet-1',
    transactionId: 'tx-1',
    direction: LedgerDirection.Debit,
    money: brl('25.00'),
    balanceBefore: brl('100.00'),
    balanceAfter: brl('75.00'),
    createdAt: CREATED_AT,
    ...overrides,
  });
}

describe('WalletLedgerEntry.create — aritmética', () => {
  test('cria lançamento DEBIT balanceado (before - money === after)', () => {
    const entry = makeEntry({ direction: LedgerDirection.Debit });

    expect(entry.direction).toBe(LedgerDirection.Debit);
    expect(entry.balanceBefore.toJSON()).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(entry.money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(entry.balanceAfter.toJSON()).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(entry.isBalanced()).toBe(true);
  });

  test('cria lançamento CREDIT balanceado (before + money === after)', () => {
    const entry = makeEntry({
      direction: LedgerDirection.Credit,
      balanceAfter: brl('125.00'),
    });

    expect(entry.isBalanced()).toBe(true);
  });

  test('rejeita DEBIT desbalanceado', () => {
    expect(() => makeEntry({ direction: LedgerDirection.Debit, balanceAfter: brl('80.00') })).toThrow(
      UnbalancedLedgerEntryError,
    );
  });

  test('rejeita CREDIT desbalanceado', () => {
    expect(() =>
      makeEntry({ direction: LedgerDirection.Credit, balanceAfter: brl('75.00') }),
    ).toThrow(UnbalancedLedgerEntryError);
  });
});

describe('WalletLedgerEntry.create — validações', () => {
  test('rejeita amount zero ou negativo', () => {
    const negative = brl('0.00').subtract(brl('5.00'));

    expect(() => makeEntry({ money: Money.zero('BRL'), balanceAfter: brl('100.00') })).toThrow(
      InvalidMoneyError,
    );
    expect(() => makeEntry({ money: negative, balanceAfter: brl('105.00') })).toThrow(
      InvalidMoneyError,
    );
  });

  test('rejeita moeda da movimentação diferente do saldo', () => {
    expect(() =>
      makeEntry({ money: usd('25.00'), balanceBefore: brl('100.00'), balanceAfter: brl('75.00') }),
    ).toThrow(CurrencyMismatchError);
  });

  test('rejeita saldo final em moeda diferente', () => {
    expect(() =>
      makeEntry({ balanceBefore: brl('100.00'), balanceAfter: usd('75.00') }),
    ).toThrow(CurrencyMismatchError);
  });
});

describe('WalletLedgerEntry.rehydrate', () => {
  test('reconstrói o estado preservando os campos', () => {
    const entry = WalletLedgerEntry.rehydrate({
      id: 'ledger-9',
      walletId: 'wallet-9',
      transactionId: 'tx-9',
      direction: LedgerDirection.Credit,
      money: brl('30.00'),
      balanceBefore: brl('10.00'),
      balanceAfter: brl('40.00'),
      createdAt: CREATED_AT,
    });

    expect(entry.id).toBe('ledger-9');
    expect(entry.walletId).toBe('wallet-9');
    expect(entry.transactionId).toBe('tx-9');
    expect(entry.direction).toBe(LedgerDirection.Credit);
    expect(entry.createdAt).toBe(CREATED_AT);
    expect(entry.isBalanced()).toBe(true);
  });

  test('não revalida a aritmética de estado já persistido', () => {
    const entry = WalletLedgerEntry.rehydrate({
      id: 'ledger-10',
      walletId: 'wallet-10',
      transactionId: 'tx-10',
      direction: LedgerDirection.Debit,
      money: brl('25.00'),
      balanceBefore: brl('100.00'),
      balanceAfter: brl('999.00'), // inconsistente, mas persistido
      createdAt: CREATED_AT,
    });

    expect(entry.isBalanced()).toBe(false);
  });
});

describe('WalletLedgerEntry — imutabilidade estrutural', () => {
  test('a instância é congelada e não aceita mutação', () => {
    const entry = makeEntry();

    expect(Object.isFrozen(entry)).toBe(true);
    expect(() => {
      (entry as { walletId: string }).walletId = 'outra-wallet';
    }).toThrow(TypeError);
    expect(entry.walletId).toBe('wallet-1');
  });

  test('não expõe métodos de transição', () => {
    const entry = makeEntry();
    const asRecord = entry as unknown as Record<string, unknown>;

    expect(asRecord.markProcessed).toBeUndefined();
    expect(asRecord.reject).toBeUndefined();
    expect(asRecord.fail).toBeUndefined();
  });
});
