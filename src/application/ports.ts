import type { IntegrationEvent } from '../domain/events';
import type { Money } from '../domain/money';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';
import type { WalletLedgerEntry } from '../domain/wallet-ledger-entry';

export interface WalletRepository {
  /** `INSERT ... ON CONFLICT (player_id, currency) DO NOTHING RETURNING`; `false` = já existia. */
  insertIfAbsent(wallet: Wallet): Promise<boolean>;
  findById(id: string): Promise<Wallet | undefined>;
}

export interface TransactionStore {
  /** Persiste o estado atual; `observedBalance` vai para as colunas dedicadas do replay. */
  insert(transaction: WagerTransaction, observedBalance?: Money): Promise<void>;
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
