import type { EntityManager } from '@mikro-orm/postgresql';

import type { TransactionStore } from '../../application/ports';
import type { Money } from '../../domain/money';
import type { WagerTransaction } from '../../domain/wager-transaction';

/** Persiste o estado atual da transação (status, carimbos e saldo observado) via INSERT puro. */
export class MikroOrmTransactionStore implements TransactionStore {
  constructor(private readonly em: EntityManager) {}

  async insert(transaction: WagerTransaction, observedBalance?: Money): Promise<void> {
    await this.em.execute(
      `INSERT INTO wager_transaction
         (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
          reference_external_transaction_id, reference_transaction_id, status, failure_code,
          observed_balance_amount, observed_balance_currency, created_at, processed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
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
      ],
    );
  }
}
