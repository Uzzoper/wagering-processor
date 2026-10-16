import { defineEntity } from '@mikro-orm/core';

/**
 * Espelho fino da tabela `wager_transaction`. Só colunas + índices.
 * `kind`/`status`/`failure_code` são strings (varchar), não enums nativos do PG.
 */
export const WagerTransactionEntity = defineEntity({
  name: 'WagerTransaction',
  tableName: 'wager_transaction',
  properties: (p) => ({
    id: p.uuid().primary(),
    providerId: p.string().length(255).fieldName('provider_id'),
    externalTransactionId: p.string().length(255).fieldName('external_transaction_id'),
    idempotencyKey: p.string().length(255).fieldName('idempotency_key'),
    payloadHash: p.string().length(64).fieldName('payload_hash'),
    walletId: p.uuid().fieldName('wallet_id'),
    playerId: p.uuid().fieldName('player_id'),
    roundId: p.string().length(255).fieldName('round_id'),
    gameId: p.string().length(255).fieldName('game_id'),
    kind: p.string().length(16),
    moneyAmount: p.decimal().precision(19).scale(2).fieldName('money_amount'),
    moneyCurrency: p.string().length(3).fieldName('money_currency'),
    referenceExternalTransactionId: p
      .string()
      .length(255)
      .fieldName('reference_external_transaction_id')
      .nullable(),
    referenceTransactionId: p.uuid().fieldName('reference_transaction_id').nullable(),
    status: p.string().length(32),
    failureCode: p.string().length(64).fieldName('failure_code').nullable(),
    observedBalanceAmount: p
      .decimal()
      .precision(19)
      .scale(2)
      .fieldName('observed_balance_amount')
      .nullable(),
    observedBalanceCurrency: p.string().length(3).fieldName('observed_balance_currency').nullable(),
    retryAttempts: p.integer().fieldName('retry_attempts').default(0),
    pendingSince: p.datetime().fieldName('pending_since').nullable(),
    nextRetryAt: p.datetime().fieldName('next_retry_at').nullable(),
    createdAt: p.datetime().fieldName('created_at'),
    processedAt: p.datetime().fieldName('processed_at').nullable(),
  }),
  uniques: [
    { name: 'uq_wager_idempotency', properties: ['idempotencyKey'] },
    { name: 'uq_wager_provider_external', properties: ['providerId', 'externalTransactionId'] },
    {
      name: 'uq_wager_single_reversal',
      properties: ['referenceTransactionId'],
      where: "kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED'",
    },
  ],
  indexes: [
    { name: 'ix_wager_wallet_status', properties: ['walletId', 'status'] },
    { name: 'ix_wager_reference', properties: ['providerId', 'referenceExternalTransactionId'] },
    {
      name: 'ix_wager_pending_ref',
      properties: ['status', 'nextRetryAt'],
      where: "status = 'PENDING_REFERENCE'",
    },
  ],
});
