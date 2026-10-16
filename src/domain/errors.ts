/**
 * Erros de domínio — puros, sem dependência de Nest/ORM.
 * O mapeamento para HTTP/SQS (`failureCode`, status) acontece na borda/use case;
 * por isso os erros de negócio abaixo **não** embutem `failureCode`.
 */

import type { Money } from './money';

/** Entrada inválida em contrato de `Money` (formato de amount/currency). */
export class InvalidMoneyError extends Error {
  readonly code = 'INVALID_MONEY' as const;

  constructor(message: string) {
    super(message);
    this.name = 'InvalidMoneyError';
  }
}

/** Operação entre valores de moedas diferentes. */
export class CurrencyMismatchError extends Error {
  readonly code = 'CURRENCY_MISMATCH' as const;

  constructor(
    readonly leftCurrency: string,
    readonly rightCurrency: string,
  ) {
    super(`Currency mismatch: "${leftCurrency}" !== "${rightCurrency}"`);
    this.name = 'CurrencyMismatchError';
  }
}

/** Débito rejeitado por saldo insuficiente (ex.: `BET` sem saldo). */
export class InsufficientBalanceError extends Error {
  constructor(
    readonly walletId: string,
    readonly balance: Money,
    readonly required: Money,
  ) {
    super(
      `Wallet "${walletId}" has insufficient balance: ${balance.toString()} < ${required.toString()}`,
    );
    this.name = 'InsufficientBalanceError';
  }
}

/**
 * Reversão (`REFUND`/`ROLLBACK`) que deixaria o saldo negativo.
 * Operacionalmente distinta de uma aposta sem saldo (`InsufficientBalanceError`).
 */
export class ReversalWouldOverdrawError extends Error {
  constructor(
    readonly walletId: string,
    readonly balance: Money,
    readonly required: Money,
  ) {
    super(
      `Reversal on wallet "${walletId}" would overdraw: ${balance.toString()} < ${required.toString()}`,
    );
    this.name = 'ReversalWouldOverdrawError';
  }
}

/** Referência de reversão inválida (kind/status/dono/moeda/rodada divergentes). */
export class InvalidReferenceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidReferenceError';
  }
}

/**
 * Transição tentada a partir de um estado terminal
 * (`PROCESSED`/`REJECTED`/`FAILED` são imutáveis) — erro de programação.
 */
export class InvalidTransactionStateError extends Error {
  constructor(
    readonly currentStatus: string,
    readonly attemptedTransition: string,
  ) {
    super(`Cannot ${attemptedTransition}: transaction is in terminal state "${currentStatus}"`);
    this.name = 'InvalidTransactionStateError';
  }
}

/** Valor de `REFUND`/`ROLLBACK` diferente do valor da referência (reversão parcial fora de escopo). */
export class AmountMismatchError extends Error {
  constructor(
    readonly expected: Money,
    readonly actual: Money,
  ) {
    super(
      `Reversal amount ${actual.toString()} differs from reference amount ${expected.toString()}`,
    );
    this.name = 'AmountMismatchError';
  }
}

/** Kind sem lançamento no ledger (ex.: `LOSS`) — erro de programação. */
export class LedgerNotApplicableError extends Error {
  constructor(readonly kind: string) {
    super(`Transaction kind "${kind}" does not produce a ledger entry`);
    this.name = 'LedgerNotApplicableError';
  }
}

/** Lançamento de ledger com aritmética inconsistente: `balanceBefore ± money !== balanceAfter`. */
export class UnbalancedLedgerEntryError extends Error {
  constructor(
    readonly direction: string,
    readonly balanceBefore: Money,
    readonly money: Money,
    readonly balanceAfter: Money,
  ) {
    super(
      `Unbalanced ledger entry: ${balanceBefore.toString()} ${direction} ${money.toString()} !== ${balanceAfter.toString()}`,
    );
    this.name = 'UnbalancedLedgerEntryError';
  }
}
