import type { IntegrationEvent } from '../domain/events';
import type { Money } from '../domain/money';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';
import type { WalletLedgerEntry } from '../domain/wallet-ledger-entry';

export interface WalletRepository {
  /** `INSERT ... ON CONFLICT (player_id, currency) DO NOTHING RETURNING`; `false` = já existia. */
  insertIfAbsent(wallet: Wallet): Promise<boolean>;
  findById(id: string): Promise<Wallet | undefined>;
  /** `SELECT ... FOR UPDATE` — serializa a concorrência por `walletId`. */
  findByIdForUpdate(id: string): Promise<Wallet | undefined>;
  /** Atualiza saldo/version/updatedAt; a existência já foi checada sob lock. */
  update(wallet: Wallet): Promise<void>;
}

/** Transação lida + saldo observado guardado (para replay exato). */
export interface StoredTransaction {
  transaction: WagerTransaction;
  observedBalance: Money;
}

/** Estado de `PENDING_REFERENCE` (worker resolve depois; sem ele, tudo NULL). */
export interface PendingState {
  retryAttempts: number;
  pendingSince: Date;
  nextRetryAt: Date;
}

export interface TransactionStore {
  /** Persiste o estado atual; `observedBalance` vai para as colunas dedicadas do replay. */
  insert(transaction: WagerTransaction, observedBalance?: Money, pending?: PendingState): Promise<void>;
  /** Como `insert`, mas `ON CONFLICT DO NOTHING`: `false` = já existia (nunca captura `23505`). */
  tryInsert(
    transaction: WagerTransaction,
    observedBalance?: Money,
    pending?: PendingState,
  ): Promise<boolean>;
  /** Leva a linha `PENDING` ao estado final (permitido: `OLD.status` não é terminal). */
  update(
    transaction: WagerTransaction,
    observedBalance?: Money,
    pending?: PendingState,
  ): Promise<void>;
  findById(id: string): Promise<StoredTransaction | undefined>;
  findByIdempotencyKey(key: string): Promise<StoredTransaction | undefined>;
  findByProviderExternal(
    providerId: string,
    externalTransactionId: string,
  ): Promise<StoredTransaction | undefined>;
  /** Reversão `PROCESSED` existente da referência (no máximo uma, pelo índice parcial). */
  findProcessedReversalOf(referenceTransactionId: string): Promise<StoredTransaction | undefined>;
}

export interface LedgerStore {
  insert(entry: WalletLedgerEntry): Promise<void>;
}

export interface OutboxStore {
  /** Só INSERT — a publicação é do worker de outbox, nunca da tx. */
  insert(events: IntegrationEvent<unknown>[]): Promise<void>;
}

/** Repositórios vinculados ao mesmo `EntityManager` (e à mesma tx SQL). */
export interface Repositories {
  readonly wallets: WalletRepository;
  readonly transactions: TransactionStore;
  readonly ledger: LedgerStore;
  readonly outbox: OutboxStore;
}

export interface UnitOfWork {
  /** Tx SQL única: `READ COMMITTED` + `SET LOCAL lock_timeout = '3s'`. */
  run<T>(work: (repositories: Repositories) => Promise<T>): Promise<T>;
  /** Leitura sem transação (autocommit), para consultas simples. */
  read<T>(work: (repositories: Repositories) => Promise<T>): Promise<T>;
}

export const UNIT_OF_WORK = Symbol('UnitOfWork');
