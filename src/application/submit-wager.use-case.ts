import {
  AmountMismatchError,
  InvalidReferenceError,
  ReversalWouldOverdrawError,
} from '../domain/errors';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../domain/events';
import { FailureCodes, type FailureCode } from '../domain/failure-code';
import type { MoneyProps } from '../domain/money';
import { Money } from '../domain/money';
import type { Wallet } from '../domain/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../domain/wallet-ledger-entry';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../domain/wager-transaction';
import { payloadHash } from './payload-hash';
import type { Repositories, StoredTransaction, UnitOfWork } from './ports';

export interface SubmitWagerInput {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string | null;
  correlationId?: string;
}

export interface SubmitProcessed {
  outcome: 'PROCESSED';
  transaction: WagerTransaction;
  observedBalance: Money;
  replay: boolean;
}

export interface SubmitRejected {
  outcome: 'REJECTED';
  transaction: WagerTransaction;
  observedBalance: Money;
  replay: boolean;
  failureCode: FailureCode;
}

export interface SubmitPendingReference {
  outcome: 'PENDING_REFERENCE';
  transaction: WagerTransaction;
  observedBalance: Money;
  replay: boolean;
}

export interface SubmitWalletNotFound {
  outcome: 'WALLET_NOT_FOUND';
}

export interface SubmitIdempotencyConflict {
  outcome: 'IDEMPOTENCY_CONFLICT';
}

export type SubmitWagerResult =
  | SubmitProcessed
  | SubmitRejected
  | SubmitPendingReference
  | SubmitWalletNotFound
  | SubmitIdempotencyConflict;

/**
 * Submete uma transação de aposta. Mesma ordem e garantias do ARCHITECTURE §2:
 * existência → insert idempotente → lock → regra no domínio → persistir → commit.
 * Rejeição de negócio RETORNA resultado (nunca lança); só infra lança (rollback).
 * Assume entrada já validada pela borda (formato de `Money`, enum de kind,
 * referência presente em REFUND/ROLLBACK) — a violação aqui seria bug de borda.
 */
export class SubmitWagerTransactionUseCase {
  constructor(private readonly unitOfWork: UnitOfWork) {}

  async execute(input: SubmitWagerInput): Promise<SubmitWagerResult> {
    const money = Money.from(input.money);
    const now = new Date();
    const correlationId = input.correlationId ?? crypto.randomUUID();
    const hash = payloadHash({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.playerId,
      walletId: input.walletId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      amount: money.toJSON().amount,
      currency: money.currency,
      referenceExternalTransactionId: input.referenceExternalTransactionId ?? null,
    });

    return this.unitOfWork.run(async (repositories): Promise<SubmitWagerResult> => {
      const walletRow = await repositories.wallets.findById(input.walletId);
      if (walletRow === undefined) {
        return { outcome: 'WALLET_NOT_FOUND' };
      }

      // Vencedor insere; perdedor bloqueia no SELECT até o commit do vencedor.
      // Se o vencedor abortou (linha sumiu), tenta de novo em vez de 503.
      for (let attempt = 0; attempt < 2; attempt += 1) {
        const candidate = WagerTransaction.create({
          id: crypto.randomUUID(),
          providerId: input.providerId,
          externalTransactionId: input.externalTransactionId,
          idempotencyKey: input.idempotencyKey,
          payloadHash: hash,
          walletId: input.walletId,
          playerId: input.playerId,
          roundId: input.roundId,
          gameId: input.gameId,
          kind: input.kind,
          money,
          referenceExternalTransactionId: input.referenceExternalTransactionId ?? undefined,
          createdAt: now,
        });
        const inserted = await repositories.transactions.tryInsert(candidate);
        if (inserted) {
          return this.processNew(repositories, candidate, input, money, now, correlationId);
        }
        const existing = await this.findExisting(repositories, input);
        if (existing === undefined) {
          continue;
        }
        return this.replay(existing, hash);
      }
      throw new Error('idempotency race exhausted after rollback; retryable');
    });
  }

  /** Idempotency-Key bateu: mesmo hash = replay, senão conflito. Por external id = conflito. */
  private async findExisting(
    repositories: Repositories,
    input: SubmitWagerInput,
  ): Promise<{ stored: StoredTransaction; byKey: boolean } | undefined> {
    const byKey = await repositories.transactions.findByIdempotencyKey(input.idempotencyKey);
    if (byKey !== undefined) {
      return { stored: byKey, byKey: true };
    }
    const byExternal = await repositories.transactions.findByProviderExternal(
      input.providerId,
      input.externalTransactionId,
    );
    if (byExternal !== undefined) {
      return { stored: byExternal, byKey: false };
    }
    return undefined;
  }

  private replay(
    existing: { stored: StoredTransaction; byKey: boolean },
    hash: string,
  ): SubmitWagerResult {
    const { transaction, observedBalance } = existing.stored;
    if (!existing.byKey || !transaction.matchesPayload(hash)) {
      return { outcome: 'IDEMPOTENCY_CONFLICT' };
    }
    switch (transaction.status) {
      case WagerTransactionStatus.Processed:
        return { outcome: 'PROCESSED', transaction, observedBalance, replay: true };
      case WagerTransactionStatus.Rejected:
        return {
          outcome: 'REJECTED',
          transaction,
          observedBalance,
          replay: true,
          // Rejeitada tem `failureCode` por construção.
          failureCode: transaction.failureCode as FailureCode,
        };
      case WagerTransactionStatus.PendingReference:
        return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: true };
      default:
        // PENDING transitório visível no replay: o processamento original ainda commita.
        return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: true };
    }
  }

  private async processNew(
    repositories: Repositories,
    transaction: WagerTransaction,
    input: SubmitWagerInput,
    money: Money,
    now: Date,
    correlationId: string,
  ): Promise<SubmitWagerResult> {
    const wallet = await repositories.wallets.findByIdForUpdate(input.walletId);
    if (wallet === undefined) {
      // Impossível: sem DELETE no schema e a existência foi checada acima.
      throw new Error('wallet vanished under lock');
    }
    if (wallet.playerId !== input.playerId) {
      return this.reject(
        repositories,
        transaction,
        wallet.balance,
        FailureCodes.WalletOwnerMismatch,
        now,
        correlationId,
      );
    }
    if (wallet.currency !== money.currency) {
      return this.reject(
        repositories,
        transaction,
        wallet.balance,
        FailureCodes.CurrencyMismatch,
        now,
        correlationId,
      );
    }

    switch (input.kind) {
      case WagerTransactionKind.Bet: {
        // Dentro do lock, checar antes de debitar é seguro (sem condição de corrida).
        if (wallet.balance.isLessThan(money)) {
          return this.reject(
            repositories,
            transaction,
            wallet.balance,
            FailureCodes.InsufficientBalance,
            now,
            correlationId,
          );
        }
        return this.applyMovement(
          repositories,
          transaction,
          wallet,
          wallet.debit(money),
          undefined,
          now,
          correlationId,
        );
      }
      case WagerTransactionKind.Win: {
        const referenceId = await this.linkWinReference(repositories, input);
        return this.applyMovement(
          repositories,
          transaction,
          wallet,
          wallet.credit(money),
          referenceId,
          now,
          correlationId,
        );
      }
      case WagerTransactionKind.Loss: {
        transaction.markProcessed(undefined, now);
        await repositories.transactions.update(transaction, wallet.balance);
        await repositories.outbox.insert([
          WagerTransactionProcessed.from(transaction, wallet.balance, { correlationId }),
        ]);
        return { outcome: 'PROCESSED', transaction, observedBalance: wallet.balance, replay: false };
      }
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Rollback: {
        return this.applyReversal(repositories, transaction, input, wallet, money, now, correlationId);
      }
      default: {
        // `OPENING` e kinds desconhecidos são barrados na borda (400).
        throw new Error(`unsupported kind in use case: ${input.kind as string}`);
      }
    }
  }

  /**
   * Referência do WIN é opcional e informativa: vincula se resolver para uma
   * transação `PROCESSED`, senão segue sem vínculo (nunca `PENDING_REFERENCE`).
   */
  private async linkWinReference(
    repositories: Repositories,
    input: SubmitWagerInput,
  ): Promise<string | undefined> {
    if (input.referenceExternalTransactionId == null) {
      return undefined;
    }
    const reference = await repositories.transactions.findByProviderExternal(
      input.providerId,
      input.referenceExternalTransactionId,
    );
    if (reference === undefined || reference.transaction.status !== WagerTransactionStatus.Processed) {
      return undefined;
    }
    return reference.transaction.id;
  }

  private async applyReversal(
    repositories: Repositories,
    transaction: WagerTransaction,
    input: SubmitWagerInput,
    wallet: Wallet,
    money: Money,
    now: Date,
    correlationId: string,
  ): Promise<SubmitWagerResult> {
    const reference = await repositories.transactions.findByProviderExternal(
      input.providerId,
      input.referenceExternalTransactionId as string,
    );
    if (
      reference === undefined ||
      reference.transaction.status === WagerTransactionStatus.PendingReference
    ) {
      return this.deferPendingReference(repositories, transaction, wallet.balance, now, correlationId);
    }
    if (
      reference.transaction.status === WagerTransactionStatus.Rejected ||
      reference.transaction.status === WagerTransactionStatus.Failed
    ) {
      return this.reject(
        repositories,
        transaction,
        wallet.balance,
        FailureCodes.InvalidReference,
        now,
        correlationId,
      );
    }
    const alreadyReversed = await repositories.transactions.findProcessedReversalOf(
      reference.transaction.id,
    );
    if (alreadyReversed !== undefined) {
      return this.reject(
        repositories,
        transaction,
        wallet.balance,
        FailureCodes.AlreadyReversed,
        now,
        correlationId,
      );
    }
    try {
      transaction.ensureValidReversal(reference.transaction, wallet.balance);
    } catch (error) {
      if (error instanceof AmountMismatchError) {
        return this.reject(
          repositories,
          transaction,
          wallet.balance,
          FailureCodes.AmountMismatch,
          now,
          correlationId,
        );
      }
      if (error instanceof ReversalWouldOverdrawError) {
        return this.reject(
          repositories,
          transaction,
          wallet.balance,
          FailureCodes.ReversalWouldOverdraw,
          now,
          correlationId,
        );
      }
      if (error instanceof InvalidReferenceError) {
        return this.reject(
          repositories,
          transaction,
          wallet.balance,
          FailureCodes.InvalidReference,
          now,
          correlationId,
        );
      }
      throw error;
    }
    const direction = transaction.ledgerDirectionFor(reference.transaction);
    const movement =
      direction === LedgerDirection.Debit ? wallet.debit(money) : wallet.credit(money);
    return this.applyMovement(
      repositories,
      transaction,
      wallet,
      movement,
      reference.transaction.id,
      now,
      correlationId,
    );
  }

  private async applyMovement(
    repositories: Repositories,
    transaction: WagerTransaction,
    wallet: Wallet,
    movement: {
      direction: LedgerDirection;
      money: Money;
      balanceBefore: Money;
      balanceAfter: Money;
    },
    referenceTransactionId: string | undefined,
    now: Date,
    correlationId: string,
  ): Promise<SubmitProcessed> {
    transaction.markProcessed(referenceTransactionId, now);
    await repositories.wallets.update(wallet);
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: wallet.id,
      transactionId: transaction.id,
      direction: movement.direction,
      money: movement.money,
      balanceBefore: movement.balanceBefore,
      balanceAfter: movement.balanceAfter,
      createdAt: now,
    });
    await repositories.ledger.insert(entry);
    await repositories.transactions.update(transaction, movement.balanceAfter);
    await repositories.outbox.insert([
      WagerTransactionProcessed.from(transaction, movement.balanceAfter, { correlationId }),
      WalletBalanceChanged.from(wallet, entry, { correlationId }),
    ]);
    return { outcome: 'PROCESSED', transaction, observedBalance: movement.balanceAfter, replay: false };
  }

  private async reject(
    repositories: Repositories,
    transaction: WagerTransaction,
    observedBalance: Money,
    failureCode: FailureCode,
    now: Date,
    correlationId: string,
  ): Promise<SubmitRejected> {
    transaction.reject(failureCode);
    await repositories.transactions.update(transaction, observedBalance);
    await repositories.outbox.insert([
      WagerTransactionRejected.from(transaction, observedBalance, { correlationId }),
    ]);
    return { outcome: 'REJECTED', transaction, observedBalance, replay: false, failureCode };
  }

  private async deferPendingReference(
    repositories: Repositories,
    transaction: WagerTransaction,
    observedBalance: Money,
    now: Date,
    correlationId: string,
  ): Promise<SubmitPendingReference> {
    transaction.markPendingReference();
    await repositories.transactions.update(transaction, observedBalance, {
      retryAttempts: 0,
      pendingSince: now,
      nextRetryAt: new Date(now.getTime() + 30_000),
    });
    await repositories.outbox.insert([
      WagerTransactionPendingReference.from(transaction, observedBalance, { correlationId }),
    ]);
    return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: false };
  }
}
