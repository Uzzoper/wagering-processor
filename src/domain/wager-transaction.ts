import {
  AmountMismatchError,
  InvalidReferenceError,
  InvalidTransactionStateError,
  LedgerNotApplicableError,
  ReversalWouldOverdrawError,
} from './errors';
import type { FailureCode } from './failure-code';
import type { Money } from './money';
import { LedgerDirection } from './wallet-ledger-entry';

export enum WagerTransactionKind {
  /** Interno: crédito de abertura da wallet. Nunca submetido pela API/fila. */
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

const TERMINAL_STATUSES: readonly WagerTransactionStatus[] = [
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
];

const REFUND_REFERENCE_KINDS: readonly WagerTransactionKind[] = [WagerTransactionKind.Bet];
const ROLLBACK_REFERENCE_KINDS: readonly WagerTransactionKind[] = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Refund,
];

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  /** id no provedor — não o id interno. */
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

/** Estado persistido usado por `rehydrate` — já reconstruído, sem revalidação. */
export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

/**
 * Transação de aposta. Nasce em `PENDING`; transições são explícitas.
 *
 * `PROCESSED`, `REJECTED` e `FAILED` são **terminais**: tentar transicioná-las
 * lança `InvalidTransactionStateError` (erro de programação, não de negócio).
 */
export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {}

  /** Nasce em `PENDING`. Valida a exigência de referência por kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    const referenceExternalTransactionId = normalizeReference(props.referenceExternalTransactionId);
    const requiresReference =
      props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback;

    if (requiresReference && referenceExternalTransactionId === undefined) {
      throw new InvalidReferenceError(`${props.kind} requires referenceExternalTransactionId`);
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      referenceExternalTransactionId,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  // ---- transições (lançam InvalidTransactionStateError se o estado atual for terminal)

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertNotTerminal('markProcessed');
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  markPendingReference(): void {
    this.assertNotTerminal('markPendingReference');
    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    this.assertNotTerminal('reject');
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  fail(code: FailureCode): void {
    this.assertNotTerminal('fail');
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  // ---- consultas de domínio

  isTerminal(): boolean {
    return TERMINAL_STATUSES.includes(this._status);
  }

  /** Efeito no saldo por kind: `false` apenas para `LOSS`. */
  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  /** `REFUND` e `ROLLBACK` exigem referência. */
  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /**
   * Direção do lançamento no ledger. `LOSS` não gera lançamento.
   * `ROLLBACK` inverte a direção da referência.
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Rollback: {
        if (reference === undefined) {
          throw new InvalidReferenceError('ROLLBACK requires the reference to resolve ledger direction');
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new LedgerNotApplicableError(this.kind);
    }
  }

  /**
   * Valida a reversão contra a referência (ambas instâncias já resolvidas) e o
   * saldo atual da wallet. Cobre: kind reversível, kind/status da referência,
   * mesma identidade (provider/player/wallet/round/moeda), valor igual e
   * reversão que negativaria o saldo. Não cobre "já revertida" (exige consulta
   * de persistência) nem a moeda da wallet.
   *
   * Lança erro de domínio; o mapeamento para `failureCode` é do use case.
   */
  ensureValidReversal(reference: WagerTransaction, currentBalance: Money): void {
    if (!this.requiresReference()) {
      throw new InvalidReferenceError(`${this.kind} is not a reversal operation`);
    }

    const allowedKinds =
      this.kind === WagerTransactionKind.Refund ? REFUND_REFERENCE_KINDS : ROLLBACK_REFERENCE_KINDS;

    if (!allowedKinds.includes(reference.kind)) {
      throw new InvalidReferenceError(`${this.kind} cannot reference a ${reference.kind} transaction`);
    }

    if (reference.status !== WagerTransactionStatus.Processed) {
      throw new InvalidReferenceError(
        `Reference "${reference.externalTransactionId}" is not PROCESSED (status=${reference.status})`,
      );
    }

    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.roundId !== this.roundId
    ) {
      throw new InvalidReferenceError('Reference does not belong to the same provider/player/wallet/round');
    }

    if (reference.money.currency !== this.money.currency) {
      throw new InvalidReferenceError('Reference currency differs from the reversal currency');
    }

    if (!reference.money.equals(this.money)) {
      throw new AmountMismatchError(reference.money, this.money);
    }

    const direction = this.ledgerDirectionFor(reference);
    if (direction === LedgerDirection.Debit && currentBalance.isLessThan(this.money)) {
      throw new ReversalWouldOverdrawError(this.walletId, currentBalance, this.money);
    }
  }

  private assertNotTerminal(transition: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this._status, transition);
    }
  }
}

function normalizeReference(value: string | undefined): string | undefined {
  if (value === undefined || value.trim().length === 0) return undefined;
  return value;
}
