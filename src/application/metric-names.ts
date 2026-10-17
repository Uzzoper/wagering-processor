/** Nomes canônicos das métricas (PADRÃO underscore; CHALLENGE §12). */

/** Transações submetidas por `status` (PROCESSED/REJECTED/PENDING_REFERENCE) e `kind`. */
export const WAGER_TRANSACTIONS_TOTAL = 'wager_transactions_total';

/** Duplicatas detectadas: `source=idempotency` (replay HTTP) ou `source=inbox` (SQS). */
export const WAGER_DUPLICATES_TOTAL = 'wager_duplicates_total';

/** Entregas SQS reorderadas: `ApproximateReceiveCount >= 2` no processamento. */
export const SQS_RETRIES_TOTAL = 'sqs_retries_total';

/** Conflitos de lock no Postgres (55P03/40P01) — a UnitOfWork segue rethrowando. */
export const DB_LOCK_CONFLICTS_TOTAL = 'db_lock_conflicts_total';

/** Falha de publicação da outbox antes da agendada (backoff por scheduleRetry). */
export const OUTBOX_PUBLISH_ERRORS_TOTAL = 'outbox_publish_errors_total';
