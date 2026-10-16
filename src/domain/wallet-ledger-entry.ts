import {
  CurrencyMismatchError,
  InvalidMoneyError,
  UnbalancedLedgerEntryError,
} from './errors';
import type { Money } from './money';

/** Sentido do lançamento no ledger. */
export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export type LedgerEntryState = CreateLedgerEntryProps;

/**
 * Lançamento no ledger de uma wallet — **estruturalmente imutável**:
 * só campos `readonly`, sem setters nem métodos de transição. Um lançamento,
 * uma vez criado, nunca é alterado nem removido (ARCHITECTURE.md §8).
 *
 * `create` valida a aritmética (`balanceBefore ± money === balanceAfter`),
 * que `money > 0` e que as três moedas coincidem. `rehydrate` reconstrói
 * estado já persistido sem revalidar.
 */
export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.money.isPositive()) {
      throw new InvalidMoneyError('ledger entry amount must be positive');
    }
    assertSameCurrency(props.money, props.balanceBefore, props.balanceAfter);

    const expectedBalanceAfter =
      props.direction === LedgerDirection.Debit
        ? props.balanceBefore.subtract(props.money)
        : props.balanceBefore.add(props.money);

    if (!expectedBalanceAfter.equals(props.balanceAfter)) {
      throw new UnbalancedLedgerEntryError(
        props.direction,
        props.balanceBefore,
        props.money,
        props.balanceAfter,
      );
    }

    return new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt,
    );
  }

  /** Reconstrução a partir da persistência — não revalida aritmética. */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /** `balanceBefore ± money === balanceAfter` (débito subtrai, crédito soma). */
  isBalanced(): boolean {
    const expected =
      this.direction === LedgerDirection.Debit
        ? this.balanceBefore.subtract(this.money)
        : this.balanceBefore.add(this.money);
    return expected.equals(this.balanceAfter);
  }
}

function assertSameCurrency(money: Money, before: Money, after: Money): void {
  if (money.currency !== before.currency) {
    throw new CurrencyMismatchError(money.currency, before.currency);
  }
  if (money.currency !== after.currency) {
    throw new CurrencyMismatchError(money.currency, after.currency);
  }
}
