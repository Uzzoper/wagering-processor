import { describe, expect, test } from 'bun:test';

import {
  CurrencyMismatchError,
  InsufficientBalanceError,
  InvalidMoneyError,
} from '../../src/domain/errors';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';
import { LedgerDirection } from '../../src/domain/wallet-ledger-entry';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
const usd = (amount: string): Money => Money.from({ amount, currency: 'USD' });

const AT = new Date('2026-03-01T12:00:00.000Z');

function openWallet(initial = '100.00'): Wallet {
  return Wallet.open({ id: 'wallet-1', playerId: 'player-1', initialBalance: brl(initial) });
}

describe('Wallet.open', () => {
  test('cria wallet com version 1 e saldo inicial', () => {
    const wallet = openWallet('100.00');

    expect(wallet.id).toBe('wallet-1');
    expect(wallet.playerId).toBe('player-1');
    expect(wallet.currency).toBe('BRL');
    expect(wallet.balance.toJSON()).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);
  });

  test('moeda da wallet nasce do Money já validado (modelo multi-moeda)', () => {
    const wallet = Wallet.open({ id: 'wallet-2', playerId: 'player-2', initialBalance: usd('10.00') });

    expect(wallet.currency).toBe('USD');
    expect(wallet.balance.toJSON()).toEqual({ amount: '10.00', currency: 'USD' });
  });

  test('createdAt e updatedAt são preenchidos e coincidem na criação', () => {
    const wallet = openWallet();

    expect(wallet.createdAt).toBeInstanceOf(Date);
    expect(wallet.updatedAt).toBeInstanceOf(Date);
    expect(wallet.updatedAt.getTime()).toBe(wallet.createdAt.getTime());
  });

  test('aceita saldo inicial zero', () => {
    const wallet = openWallet('0.00');

    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
  });

  test('rejeita saldo inicial negativo', () => {
    const negative = brl('0.00').subtract(brl('1.00'));

    expect(() =>
      Wallet.open({ id: 'w', playerId: 'p', initialBalance: negative }),
    ).toThrow(InvalidMoneyError);
  });
});

describe('Wallet.rehydrate', () => {
  test('reconstrói o estado persistido sem revalidar nem alterar', () => {
    const createdAt = new Date('2025-01-01T00:00:00.000Z');
    const updatedAt = new Date('2025-06-01T00:00:00.000Z');

    const wallet = Wallet.rehydrate({
      id: 'wallet-9',
      playerId: 'player-9',
      currency: 'BRL',
      balance: brl('42.00'),
      version: 7,
      createdAt,
      updatedAt,
    });

    expect(wallet.id).toBe('wallet-9');
    expect(wallet.balance.toJSON()).toEqual({ amount: '42.00', currency: 'BRL' });
    expect(wallet.version).toBe(7);
    expect(wallet.createdAt).toBe(createdAt);
    expect(wallet.updatedAt).toBe(updatedAt);
  });
});

describe('Wallet.debit', () => {
  test('debita saldo, incrementa version e devolve a movimentação', () => {
    const wallet = openWallet('100.00');

    const movement = wallet.debit(brl('25.00'), AT);

    expect(wallet.balance.toJSON()).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(wallet.version).toBe(2);
    expect(wallet.updatedAt).toBe(AT);
    expect(movement).toEqual({
      direction: LedgerDirection.Debit,
      money: brl('25.00'),
      balanceBefore: brl('100.00'),
      balanceAfter: brl('75.00'),
      walletVersion: 2,
    });
  });

  test('permite debitar exatamente o saldo (fica zero, nunca negativo)', () => {
    const wallet = openWallet('80.00');

    wallet.debit(brl('80.00'), AT);

    expect(wallet.balance.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(wallet.balance.isNegative()).toBe(false);
  });

  test('rejeita débito acima do saldo com InsufficientBalanceError e não altera estado', () => {
    const wallet = openWallet('100.00');

    expect(() => wallet.debit(brl('100.01'), AT)).toThrow(InsufficientBalanceError);
    expect(wallet.balance.toJSON()).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);
    expect(wallet.updatedAt.getTime()).toBe(wallet.createdAt.getTime());
  });

  test('o erro carrega wallet, saldo e valor exigido', () => {
    const wallet = openWallet('100.00');

    try {
      wallet.debit(brl('150.00'), AT);
      expect.unreachable('deveria ter lançado InsufficientBalanceError');
    } catch (error) {
      expect(error).toBeInstanceOf(InsufficientBalanceError);
      const insufficient = error as InsufficientBalanceError;
      expect(insufficient.walletId).toBe('wallet-1');
      expect(insufficient.balance.equals(brl('100.00'))).toBe(true);
      expect(insufficient.required.equals(brl('150.00'))).toBe(true);
    }
  });

  test('rejeita débito em moeda diferente', () => {
    const wallet = openWallet('100.00');

    expect(() => wallet.debit(usd('10.00'), AT)).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test('rejeita débito de valor não positivo', () => {
    const wallet = openWallet('100.00');
    const negative = brl('0.00').subtract(brl('5.00'));

    expect(() => wallet.debit(Money.zero('BRL'), AT)).toThrow(InvalidMoneyError);
    expect(() => wallet.debit(negative, AT)).toThrow(InvalidMoneyError);
    expect(wallet.version).toBe(1);
  });
});

describe('Wallet.credit', () => {
  test('credita saldo, incrementa version e devolve a movimentação', () => {
    const wallet = openWallet('100.00');

    const movement = wallet.credit(brl('50.00'), AT);

    expect(wallet.balance.toJSON()).toEqual({ amount: '150.00', currency: 'BRL' });
    expect(wallet.version).toBe(2);
    expect(wallet.updatedAt).toBe(AT);
    expect(movement).toEqual({
      direction: LedgerDirection.Credit,
      money: brl('50.00'),
      balanceBefore: brl('100.00'),
      balanceAfter: brl('150.00'),
      walletVersion: 2,
    });
  });

  test('rejeita crédito em moeda diferente', () => {
    const wallet = openWallet('100.00');

    expect(() => wallet.credit(usd('10.00'), AT)).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test('rejeita crédito de valor não positivo', () => {
    const wallet = openWallet('100.00');

    expect(() => wallet.credit(Money.zero('BRL'), AT)).toThrow(InvalidMoneyError);
    expect(wallet.version).toBe(1);
  });
});

describe('Wallet — version só muda quando o saldo muda', () => {
  test('incrementa a cada movimentação aplicada', () => {
    const wallet = openWallet('100.00');

    expect(wallet.version).toBe(1);
    wallet.credit(brl('10.00'), AT);
    expect(wallet.version).toBe(2);
    wallet.debit(brl('5.00'), AT);
    expect(wallet.version).toBe(3);
    expect(wallet.balance.toJSON()).toEqual({ amount: '105.00', currency: 'BRL' });
  });

  test('não incrementa em operações rejeitadas', () => {
    const wallet = openWallet('10.00');

    expect(() => wallet.debit(brl('10.01'), AT)).toThrow(InsufficientBalanceError);
    expect(() => wallet.debit(usd('1.00'), AT)).toThrow(CurrencyMismatchError);
    expect(() => wallet.credit(Money.zero('BRL'), AT)).toThrow(InvalidMoneyError);

    expect(wallet.version).toBe(1);
    expect(wallet.balance.toJSON()).toEqual({ amount: '10.00', currency: 'BRL' });
  });
});
