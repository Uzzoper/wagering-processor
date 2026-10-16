import { CurrencyMismatchError, InsufficientBalanceError, InvalidMoneyError } from './errors';
import { Money } from './money';
import { LedgerDirection } from './wallet-ledger-entry';

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
}

/** Estado persistido usado por `rehydrate` — já reconstruído, sem revalidação. */
export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/** Movimentação aplicada — insumo direto para o lançamento no ledger e o evento. */
export interface WalletMovement {
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly walletVersion: number;
}

/**
 * Aggregate root da wallet. Estado encapsulado; só `debit`/`credit` movem saldo.
 *
 * Invariantes: saldo nunca negativo; toda mudança de saldo incrementa `version`
 * (que nasce em `1`); operação de moeda diferente é erro; `open` rejeita saldo
 * inicial negativo. Não há concorrência em memória — o lock por `walletId` é do
 * banco (ARCHITECTURE.md §3).
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError('initialBalance must not be negative');
    }
    const now = new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(money: Money, at: Date = new Date()): WalletMovement {
    return this.apply(LedgerDirection.Debit, money, at);
  }

  credit(money: Money, at: Date = new Date()): WalletMovement {
    return this.apply(LedgerDirection.Credit, money, at);
  }

  private apply(direction: LedgerDirection, money: Money, at: Date): WalletMovement {
    this.assertSameCurrency(money);
    if (!money.isPositive()) {
      throw new InvalidMoneyError('movement amount must be positive');
    }
    if (direction === LedgerDirection.Debit && this._balance.isLessThan(money)) {
      throw new InsufficientBalanceError(this.id, this._balance, money);
    }

    const balanceBefore = this._balance;
    const balanceAfter =
      direction === LedgerDirection.Debit
        ? balanceBefore.subtract(money)
        : balanceBefore.add(money);

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = at;

    return { direction, money, balanceBefore, balanceAfter, walletVersion: this._version };
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
