import type { IntegrationEvent } from '../domain/events';
import type { LedgerCursor } from './ledger-cursor';
import type { InboxMessage } from '../domain/inbox-message';
import type { LedgerDirection } from '../domain/wallet-ledger-entry';
import type { Money } from '../domain/money';
import type { OutboxMessage } from '../domain/outbox-message';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';
import type { WalletLedgerEntry } from '../domain/wallet-ledger-entry';

/** Fonte de tempo injetável (testes usam clock fake; nunca sleep real). */
export interface Clock {
  now(): Date;
}

export interface WalletRepository {
  /** `INSERT ... ON CONFLICT (player_id, currency) DO NOTHING RETURNING`; `false` = já existia. */
  insertIfAbsent(wallet: Wallet): Promise<boolean>;
  findById(id: string): Promise<Wallet | undefined>;
  /** `SELECT ... FOR UPDATE` — serializa a concorrência por `walletId`. */
  findByIdForUpdate(id: string): Promise<Wallet | undefined>;
  /** Atualiza saldo/version/updatedAt; a existência já foi checada sob lock. */
  update(wallet: Wallet): Promise<void>;
}

/** Transação lida + saldo observado guardado + estado pendente (worker 7c). */
export interface StoredTransaction {
  transaction: WagerTransaction;
  observedBalance: Money;
  pending?: PendingState;
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
  /**
   * Claim de linhas `PENDING_REFERENCE` vencidas com lease + `SKIP LOCKED`
   * (arquétipo do claim da outbox §5): adianta `next_retry_at` em `leaseMs`.
   */
  claimPendingReference(limit: number, now: Date, leaseMs: number): Promise<StoredTransaction[]>;
}

export interface LedgerStore {
  insert(entry: WalletLedgerEntry): Promise<void>;
}

export interface OutboxStore {
  /** Só INSERT — a publicação é do worker de outbox, nunca da tx financeira. */
  insert(events: IntegrationEvent<unknown>[]): Promise<void>;
  /**
   * Claim com lease em tx curta: `published_at IS NULL AND next_attempt_at <= now()`
   * em `FOR UPDATE SKIP LOCKED`, adiantando `next_attempt_at` em `leaseMs`.
   * Concorrentes nunca recebem a mesma linha no mesmo momento.
   */
  claimDue(limit: number, leaseMs: number): Promise<OutboxMessage[]>;
  /** Grava `published_at = now()` após a publicação (idempotente, tx curta). */
  markPublished(id: string): Promise<void>;
  /** Persiste attempts + `next_attempt_at` calculados por `OutboxMessage.scheduleRetry`. */
  persistRetry(id: string, attempts: number, nextAttemptAt: Date): Promise<void>;
}

/** Evento pronto para o broker: envelope serializado + chaves de rota/dedup SQS FIFO. */
export interface PublishRequest {
  eventId: string;
  aggregateId: string;
  /** Envelope `IntegrationEvent.toJSON()` serializado (JSON string). */
  payload: string;
}

/** A única porta que publica depois do commit — usada só pelo worker da outbox. */
export interface EventPublisherPort {
  publish(request: PublishRequest): Promise<void>;
}

export interface InboxStore {
  /**
   * Dedup persistente do consumer SQS, dento da MESMA tx do use case:
   * `INSERT ... ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING`
   * — `false` = redelivery da mensagem (nunca captura `23505`: abortaria a tx).
   */
  tryInsert(message: InboxMessage): Promise<boolean>;
}

/** Repositórios vinculados ao mesmo `EntityManager` (e à mesma tx SQL). */
export interface Repositories {
  readonly wallets: WalletRepository;
  readonly transactions: TransactionStore;
  readonly ledger: LedgerStore;
  readonly outbox: OutboxStore;
  readonly inbox: InboxStore;
}

export interface UnitOfWork {
  /** Tx SQL única: `READ COMMITTED` + `SET LOCAL lock_timeout = '3s'`. */
  run<T>(work: (repositories: Repositories) => Promise<T>): Promise<T>;
  /** Leitura sem transação (autocommit), para consultas simples. */
  read<T>(work: (repositories: Repositories) => Promise<T>): Promise<T>;
}

/** Linha do ledger para a API (`GET /wallets/:id/ledger`), dinheiro como `Money`. */
export interface LedgerEntryRow {
  id: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

/** Página keyset do ledger (`(created_at, id) ASC`); vazia quando nada venceu o cursor. */
export interface LedgerPage {
  readonly items: LedgerEntryRow[];
  /**
   * ISO-UTC com microssegundos da ÚLTIMA linha devolvida, presente só quando
   * existe próxima página (look-ahead de `limit+1` no store; o Date do item
   * perde 3 dígitos e quebraria o keyset no empate de ms).
   */
  readonly cursorCreatedAt: string | undefined;
}

/** Retrato da reconciliação vindo de UMA query SQL (NUMERIC como texto). */
export interface WalletReconciliationData {
  /** Saldo materializado (`Money` — o CHECK garante ≥ 0). */
  storedBalance: Money;
  /** Soma `CREDIT − DEBIT` do ledger (NUMERIC texto de 2 casas). */
  calculatedAmount: string;
  /** `stored − calculated` (NUMERIC texto de 2 casas; pode ser negativa). */
  difference: string;
  checkedEntries: number;
}

export interface WalletReadStore {
  /**
   * Página keyset do ledger. `undefined` = wallet inexistente (mesma
   * semântica do `GET /wallets/:id`).
   */
  ledgerPage(
    walletId: string,
    cursor: LedgerCursor | undefined,
    limit: number,
  ): Promise<LedgerPage | undefined>;
  /** Reconciliação em UMA query (a query devolve 0 linhas → 404 na borda). */
  reconcile(walletId: string): Promise<WalletReconciliationData | undefined>;
}

/** Rótulos de uma série de métrica (ex.: `{ status: 'PROCESSED', kind: 'BET' }`). */
export type MetricLabels = Readonly<Record<string, string>>;

/** Amostra de uma série (counter/gauge) pronta para exposição. */
export interface MetricSample {
  readonly labels: MetricLabels;
  readonly value: number;
}

/**
 * Histograma pronto para exposição: uma série por combinação de labels,
 * render como `_bucket{le=...}` + `_sum` + `_count` (acumulativo do Prometheus).
 */
export interface MetricHistogram {
  readonly labels: MetricLabels;
  readonly buckets: readonly number[];
  /** Cumulativa por bucket (`le`): quanto de amostras caiu ATÉ o bucket. */
  readonly bucketCounts: readonly [number, number][];
  readonly sum: number;
  readonly count: number;
}

export interface MetricFamily {
  /** Nome canonical (sem labels): base do HELP/TYPE da exposição. */
  readonly name: string;
  readonly type: 'counter' | 'gauge' | 'histogram';
  /** Counter/gauge: amostras; histograma: histogramas por série de labels. */
  readonly samples?: readonly MetricSample[];
  readonly histograms?: readonly MetricHistogram[];
}

/**
 * Porta de métricas (fatia 8b): registries em memória — counter com labels,
 * histograma de latência e gauge computado no scrape. A exposição Prometheus é
 * responsabilidade da borda (`/metrics`), não de quem instrumenta.
 */
export interface MetricsPort {
  /** Incrementa o counter `name[labels]` (+1). */
  increment(name: string, labels?: MetricLabels): void;
  /** Leitura direta de counter (usado nos testes). */
  counter(name: string, labels?: MetricLabels): number;
  /** Observação de latência (histograma). */
  observe(name: string, valueMs: number): void;
  /** Gauge computado no scrape (p. ex. GAUGE de DLQ/outbox lag com cache). */
  setGauge(name: string, read: () => Promise<number>): void;
  /** Retrato completo para a borda `/metrics`. */
  snapshot(): Promise<readonly MetricFamily[]>;
}

/** Nome travado do contador de divergência de reconciliação. */
export const RECONCILIATION_MISMATCHES_METRIC = 'reconciliation_mismatches_total';

/** Limites de página do ledger (fatia 8a). */
export const LEDGER_LIMIT_DEFAULT = 50;
export const LEDGER_LIMIT_MAX = 200;

export const UNIT_OF_WORK = Symbol('UnitOfWork');
export const EVENT_PUBLISHER = Symbol('EventPublisherPort');
export const WALLET_READ_STORE = Symbol('WalletReadStore');
export const METRICS = Symbol('MetricsPort');
