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
import {
  PENDING_REF_BACKOFF_BASE_MS,
  pendingRefBackoffMs,
  pendingRefExpired,
} from '../domain/pending-reference-policy';
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

/** Parâmetros de resolução do worker de `PENDING_REFERENCE` (motivo do resume). */
export interface PendingResolution {
  now: Date;
  ttlHours: number;
  maxAttempts: number;
}

export interface SubmitWagerContext {
  /**
   * Gancho único da entrada SQS (CHALLENGE §10): dedup persistente via inbox
   * DENTRO da mesma tx SQL (ordem §2: inbox → insert idempotente → lock…).
   * HTTP não usa; redelivery volta pelo caminho de replay do use case.
   */
  beforeRepositories?: (repositories: Repositories) => Promise<unknown>;
  /**
   * Usado apenas pelo worker 7c: faz RESUME de uma linha `PENDING_REFERENCE`
   * (executa de fato a resolução) em vez de devolver o replay `202` do HTTP.
   */
  pendingResolution?: PendingResolution;
}

/**
 * Submete uma transação de aposta. Mesma ordem e garantias do ARCHITECTURE §2:
 * existência → insert idempotente → lock → regra no domínio → persistir → commit.
 * Rejeição de negócio RETORNA resultado (nunca lança); só infra lança (rollback).
 * Assume entrada já validada pela borda (formato de `Money`, enum de kind,
 * referência presente em REFUND/ROLLBACK) — a violação aqui seria bug de borda.
 */
export class SubmitWagerTransactionUseCase {
  constructor(private readonly unitOfWork: UnitOfWork) {}

  async execute(
    input: SubmitWagerInput,
    context?: SubmitWagerContext,
  ): Promise<SubmitWagerResult> {
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
      // Inbox (só SQS) é o passo 1 da ordem travada — antes de qualquer SQL de negócio.
      await context?.beforeRepositories?.(repositories);

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
        return this.replay(existing, hash, input, repositories, context);
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

  private async replay(
    existing: { stored: StoredTransaction; byKey: boolean },
    hash: string,
    input: SubmitWagerInput,
    repositories: Repositories,
    context?: SubmitWagerContext,
  ): Promise<SubmitWagerResult> {
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
        // Sem opções de resolução (HTTP): o worker ainda não resolveu — replay 202.
        if (context?.pendingResolution === undefined) {
          return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: true };
        }
        // Worker 7c: reprocessa de verdade pelo mesmo caminho ("resume", §5).
        return this.resumePendingReference(
          repositories,
          existing.stored,
          context.pendingResolution,
          input.correlationId ?? crypto.randomUUID(),
        );
      default:
        // PENDING transitório visível no replay: o processamento original ainda commita.
        return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: true };
    }
  }

  /**
   * Resume do worker 7c dentro da MESMA tx: lock da wallet → re-leitura
   * anti-corrida → expirou rejeita, senão reaplica a reversão. Nunca confia na
   * cópia claimada: a linha fresca sob o lock é a fonte da verdade.
   */
  private async resumePendingReference(
    repositories: Repositories,
    stored: StoredTransaction,
    resolution: PendingResolution,
    correlationId: string,
  ): Promise<SubmitWagerResult> {
    const wallet = await repositories.wallets.findByIdForUpdate(
      stored.transaction.walletId,
    );
    if (wallet === undefined) {
      // Impossível: sem DELETE no schema (mesmo motivo de processNew).
      throw new Error('wallet vanished under lock');
    }

    // Anti-corrida: pode ter virado terminal entre o claim e o lock da wallet.
    const fresh = await repositories.transactions.findById(stored.transaction.id);
    if (fresh === undefined) {
      throw new Error('pending transaction vanished under lock');
    }
    if (fresh.transaction.isTerminal()) {
      return this.replayTerminal(fresh);
    }

    // stored.pending por construção (linha veio do claim por status).
    const pending = fresh.pending!;
    if (
      pendingRefExpired(
        pending.pendingSince,
        pending.retryAttempts,
        resolution.now,
        resolution.ttlHours,
        resolution.maxAttempts,
      )
    ) {
      return this.reject(
        repositories,
        fresh.transaction,
        wallet.balance,
        FailureCodes.ReferenceNotFound,
        resolution.now,
        correlationId,
      );
    }

    return this.applyReversal(
      repositories,
      fresh.transaction,
      this.reversalInput(fresh.transaction),
      wallet,
      fresh.transaction.money,
      resolution.now,
      correlationId,
      {
        retryAttempts: pending.retryAttempts,
        pendingSince: pending.pendingSince,
      },
    );
  }

  /** Replay exato de um terminal já resolvido (nada duplica). */
  private replayTerminal(stored: StoredTransaction): SubmitRejected | SubmitProcessed {
    const { transaction, observedBalance } = stored;
    if (transaction.status === WagerTransactionStatus.Rejected) {
      return {
        outcome: 'REJECTED',
        transaction,
        observedBalance,
        replay: true,
        failureCode: transaction.failureCode as FailureCode,
      };
    }
    return { outcome: 'PROCESSED', transaction, observedBalance, replay: true };
  }

  /** Reconstrói o input de reversão a partir dos campos da própria linha. */
  private reversalInput(transaction: WagerTransaction): SubmitWagerInput {
    return {
      idempotencyKey: transaction.idempotencyKey,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      playerId: transaction.playerId,
      walletId: transaction.walletId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      money: transaction.money.toJSON(),
      referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
    };
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
    resumeDefer?: { retryAttempts: number; pendingSince: Date },
  ): Promise<SubmitWagerResult> {
    const reference = await repositories.transactions.findByProviderExternal(
      input.providerId,
      input.referenceExternalTransactionId as string,
    );
    if (
      reference === undefined ||
      reference.transaction.status === WagerTransactionStatus.PendingReference
    ) {
      // Referência ausente/ainda pendente: primeiro defer emite evento; renovação do worker não.
      if (resumeDefer === undefined) {
        return this.deferPendingReference(
          repositories,
          transaction,
          wallet.balance,
          now,
          correlationId,
        );
      }
      return this.renewPendingReference(
        repositories,
        transaction,
        wallet.balance,
        now,
        resumeDefer,
      );
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
      nextRetryAt: new Date(now.getTime() + PENDING_REF_BACKOFF_BASE_MS),
    });
    // Só o PRIMEIRO defer emite WagerTransactionPendingReference (renovação não duplica).
    await repositories.outbox.insert([
      WagerTransactionPendingReference.from(transaction, observedBalance, { correlationId }),
    ]);
    return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: false };
  }

  /**
   * Renovação pelo worker 7c (resume): attempts+1 com backoff exponencial com
   * jitter; SEM evento na outbox (o PendingReference só nasce no primeiro defer).
   */
  private async renewPendingReference(
    repositories: Repositories,
    transaction: WagerTransaction,
    observedBalance: Money,
    now: Date,
    resumeDefer: { retryAttempts: number; pendingSince: Date },
  ): Promise<SubmitPendingReference> {
    const attempts = resumeDefer.retryAttempts + 1;
    const nextRetryAt = new Date(now.getTime() + pendingRefBackoffMs(attempts));
    await repositories.transactions.update(transaction, observedBalance, {
      retryAttempts: attempts,
      pendingSince: resumeDefer.pendingSince,
      nextRetryAt,
    });
    return { outcome: 'PENDING_REFERENCE', transaction, observedBalance, replay: false };
  }
}
