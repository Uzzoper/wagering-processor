import type { EntityManager } from '@mikro-orm/postgresql';

import type {
  PendingState,
  StoredTransaction,
  TransactionStore,
} from '../../application/ports';
import { Money } from '../../domain/money';
import type { FailureCode } from '../../domain/failure-code';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wager-transaction';

const INSERT_COLUMNS = `(id, provider_id, external_transaction_id, idempotency_key, payload_hash,
  wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
  reference_external_transaction_id, reference_transaction_id, status, failure_code,
  observed_balance_amount, observed_balance_currency, created_at, processed_at,
  retry_attempts, pending_since, next_retry_at)`;

const SELECT_COLUMNS = `${INSERT_COLUMNS.slice(1, -1)}`;

function insertValues(
  transaction: WagerTransaction,
  observedBalance: Money | undefined,
  pending: PendingState | undefined,
): unknown[] {
  return [
    transaction.id,
    transaction.providerId,
    transaction.externalTransactionId,
    transaction.idempotencyKey,
    transaction.payloadHash,
    transaction.walletId,
    transaction.playerId,
    transaction.roundId,
    transaction.gameId,
    transaction.kind,
    transaction.money.toJSON().amount,
    transaction.money.currency,
    transaction.referenceExternalTransactionId ?? null,
    transaction.referenceTransactionId ?? null,
    transaction.status,
    transaction.failureCode ?? null,
    observedBalance?.toJSON().amount ?? null,
    observedBalance?.currency ?? null,
    transaction.createdAt,
    transaction.processedAt ?? null,
    pending?.retryAttempts ?? 0,
    pending?.pendingSince ?? null,
    pending?.nextRetryAt ?? null,
  ];
}

function mapRow(row: Record<string, unknown>): StoredTransaction {
  const currency = row.money_currency as string;
  const transaction = WagerTransaction.rehydrate({
    id: row.id as string,
    providerId: row.provider_id as string,
    externalTransactionId: row.external_transaction_id as string,
    idempotencyKey: row.idempotency_key as string,
    payloadHash: row.payload_hash as string,
    walletId: row.wallet_id as string,
    playerId: row.player_id as string,
    roundId: row.round_id as string,
    gameId: row.game_id as string,
    kind: row.kind as WagerTransactionKind,
    money: Money.from({ amount: row.money_amount as string, currency }),
    referenceExternalTransactionId:
      (row.reference_external_transaction_id as string | null) ?? undefined,
    referenceTransactionId: (row.reference_transaction_id as string | null) ?? undefined,
    status: row.status as WagerTransactionStatus,
    failureCode: (row.failure_code as FailureCode | null) ?? undefined,
    processedAt: (row.processed_at as Date | null) ?? undefined,
    createdAt: row.created_at as Date,
  });
  // Escritores sempre preenchem o saldo observado; o fallback existe só por robustez.
  const observedAmount = row.observed_balance_amount as string | null;
  const observedCurrency = (row.observed_balance_currency as string | null) ?? currency;
  const observedBalance =
    observedAmount === null
      ? Money.zero(observedCurrency)
      : Money.from({ amount: observedAmount, currency: observedCurrency });
  return { transaction, observedBalance };
}

/** INSERT puro via SQL (nunca ORM save); leitura reconstrói o domínio via `rehydrate`. */
export class MikroOrmTransactionStore implements TransactionStore {
  constructor(private readonly em: EntityManager) {}

  async insert(
    transaction: WagerTransaction,
    observedBalance?: Money,
    pending?: PendingState,
  ): Promise<void> {
    await this.em.execute(
      `INSERT INTO wager_transaction ${INSERT_COLUMNS}
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      insertValues(transaction, observedBalance, pending),
    );
  }

  async tryInsert(
    transaction: WagerTransaction,
    observedBalance?: Money,
    pending?: PendingState,
  ): Promise<boolean> {
    const rows = await this.em.execute<Array<{ id: string }>>(
      `INSERT INTO wager_transaction ${INSERT_COLUMNS}
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT DO NOTHING RETURNING id`,
      insertValues(transaction, observedBalance, pending),
    );
    return rows.length > 0;
  }

  async update(
    transaction: WagerTransaction,
    observedBalance?: Money,
    pending?: PendingState,
  ): Promise<void> {
    await this.em.execute(
      `UPDATE wager_transaction
          SET status = ?, failure_code = ?, reference_transaction_id = ?, processed_at = ?,
              observed_balance_amount = ?, observed_balance_currency = ?,
              retry_attempts = ?, pending_since = ?, next_retry_at = ?
        WHERE id = ?`,
      [
        transaction.status,
        transaction.failureCode ?? null,
        transaction.referenceTransactionId ?? null,
        transaction.processedAt ?? null,
        observedBalance?.toJSON().amount ?? null,
        observedBalance?.currency ?? null,
        pending?.retryAttempts ?? 0,
        pending?.pendingSince ?? null,
        pending?.nextRetryAt ?? null,
        transaction.id,
      ],
    );
  }

  async findById(id: string): Promise<StoredTransaction | undefined> {
    return this.findOne('id = ?', [id]);
  }

  async findByIdempotencyKey(key: string): Promise<StoredTransaction | undefined> {
    return this.findOne('idempotency_key = ?', [key]);
  }

  async findByProviderExternal(
    providerId: string,
    externalTransactionId: string,
  ): Promise<StoredTransaction | undefined> {
    return this.findOne('provider_id = ? AND external_transaction_id = ?', [
      providerId,
      externalTransactionId,
    ]);
  }

  async findProcessedReversalOf(
    referenceTransactionId: string,
  ): Promise<StoredTransaction | undefined> {
    return this.findOne(
      `reference_transaction_id = ? AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED'`,
      [referenceTransactionId],
    );
  }

  private async findOne(where: string, params: unknown[]): Promise<StoredTransaction | undefined> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT ${SELECT_COLUMNS} FROM wager_transaction WHERE ${where} LIMIT 1`,
      params,
    );
    const row = rows[0];
    if (row === undefined) {
      return undefined;
    }
    return mapRow(row);
  }
}
