import type { FailureCode } from './failure-code';
import type { Money, MoneyProps } from './money';
import type { WagerTransaction, WagerTransactionKind } from './wager-transaction';
import type { Wallet } from './wallet';
import type { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

/** Insumos comuns a todo evento de integração (o `eventId`/`occurredAt` têm default). */
export interface EventContext {
  correlationId: string;
  causationId?: string;
  /** Default: `crypto.randomUUID()`. */
  eventId?: string;
  /** Default: `new Date()`. */
  occurredAt?: Date;
}

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

/** Envelope serializado gravado no `payload` (JSONB) da outbox. */
export interface IntegrationEventJson<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string; // ISO-8601
  version: number;
  data: T;
}

/**
 * Envelope de evento de integração. `eventType` e `version` vivem na subclasse
 * (não numa string solta no call site); `data` nunca carrega instância de `Money`.
 */
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze({ ...props.data });
  }

  toJSON(): IntegrationEventJson<T> {
    const json: IntegrationEventJson<T> = {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
    if (this.causationId !== undefined) {
      json.causationId = this.causationId;
    }
    return json;
  }
}

export interface WagerTransactionProcessedData {
  transactionId: string;
  walletId: string;
  playerId: string;
  providerId: string;
  externalTransactionId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  observedBalance: MoneyProps;
}

/** Qualquer transação aplicada, inclusive `LOSS`. */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  override readonly eventType = 'WagerTransactionProcessed';
  override readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionProcessedData>) {
    super(props);
  }

  static from(
    transaction: WagerTransaction,
    observedBalance: Money,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    return new WagerTransactionProcessed({
      eventId: ctx.eventId ?? crypto.randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: transaction.id,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        observedBalance: observedBalance.toJSON(),
      },
    });
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Emitido **somente** quando o saldo muda. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  override readonly eventType = 'WalletBalanceChanged';
  override readonly version = 1;

  private constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: ctx.eventId ?? crypto.randomUUID(),
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}

export interface WagerTransactionRejectedData {
  transactionId: string;
  walletId: string;
  playerId: string;
  providerId: string;
  externalTransactionId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  failureCode: FailureCode;
  observedBalance: MoneyProps;
}

/** Rejeição de regra de negócio — `failureCode` sempre preenchido via `reject()`. */
export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  override readonly eventType = 'WagerTransactionRejected';
  override readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionRejectedData>) {
    super(props);
  }

  static from(
    transaction: WagerTransaction,
    observedBalance: Money,
    ctx: EventContext,
  ): WagerTransactionRejected {
    if (transaction.failureCode === undefined) {
      throw new Error('WagerTransactionRejected requires a failureCode (call reject() first)');
    }
    return new WagerTransactionRejected({
      eventId: ctx.eventId ?? crypto.randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: transaction.id,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        failureCode: transaction.failureCode,
        observedBalance: observedBalance.toJSON(),
      },
    });
  }
}

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  walletId: string;
  playerId: string;
  providerId: string;
  externalTransactionId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId: string;
  observedBalance: MoneyProps;
}

/** Referência ausente — reprocessada pelo worker com backoff. */
export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  override readonly eventType = 'WagerTransactionPendingReference';
  override readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionPendingReferenceData>) {
    super(props);
  }

  static from(
    transaction: WagerTransaction,
    observedBalance: Money,
    ctx: EventContext,
  ): WagerTransactionPendingReference {
    if (transaction.referenceExternalTransactionId === undefined) {
      throw new Error('WagerTransactionPendingReference requires a referenceExternalTransactionId');
    }
    return new WagerTransactionPendingReference({
      eventId: ctx.eventId ?? crypto.randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        transactionId: transaction.id,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        referenceExternalTransactionId: transaction.referenceExternalTransactionId,
        observedBalance: observedBalance.toJSON(),
      },
    });
  }
}
