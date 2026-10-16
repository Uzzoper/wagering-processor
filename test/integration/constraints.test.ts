import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import { ensureTestDatabase } from '../setup';

/**
 * Constraints contra PostgreSQL REAL (docker-compose).
 *
 * O preload `test/setup.ts` isola os testes no banco `wagering_test` (nunca o
 * `wagering` de desenvolvimento do `.env`); aqui garantimos o banco e subimos a
 * migration. Cada caso viola UMA constraint e confere `error.code` +
 * `error.constraint` (ou a mensagem, nos triggers sem constraint). No fim,
 * `down()` reverte e confirmamos que as tabelas sumiram.
 */

const TABLES = [
  'wallet',
  'wager_transaction',
  'wallet_ledger_entry',
  'inbox_message',
  'outbox_message',
];

let orm: MikroORM;

function execute(sql: string): Promise<unknown[]> {
  return orm.em.getConnection().execute(sql);
}

interface FailureExpectation {
  code: string;
  /** Nome da constraint (unique/check/FK). Ausente nos triggers e no NOT NULL. */
  constraint?: string;
  /** Coluna do erro de NOT NULL (o PG reporta `column`, não `constraint`). */
  column?: string;
  /** Substring esperada na mensagem (usada nos triggers, que não têm constraint). */
  message?: string;
}

async function expectFailure(sql: string, expected: FailureExpectation): Promise<void> {
  let caught: unknown;
  try {
    await execute(sql);
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeDefined();
  const dbError = caught as {
    code?: string;
    constraint?: string;
    column?: string;
    message?: string;
  };
  expect(dbError.code).toBe(expected.code);

  if (expected.constraint !== undefined) {
    expect(dbError.constraint).toBe(expected.constraint);
  }
  if (expected.column !== undefined) {
    expect(dbError.column).toBe(expected.column);
  }
  if (expected.message !== undefined) {
    expect(dbError.message).toContain(expected.message);
  }
}

const randomUuid = (): string => crypto.randomUUID();

// ---- SQL puro (usado nos casos de falha, sem executar) ----

interface WalletValues {
  id: string;
  playerId: string;
  currency?: string;
  balanceCurrency?: string;
  balance?: string;
  version?: number;
}

function walletSql(values: WalletValues): string {
  const currency = values.currency ?? 'BRL';
  const balanceCurrency = values.balanceCurrency ?? currency;
  const balance = values.balance ?? '0.00';
  const version = values.version ?? 1;
  return `INSERT INTO wallet (id, player_id, currency, balance_amount, balance_currency, version, created_at, updated_at)
    VALUES ('${values.id}', '${values.playerId}', '${currency}', '${balance}', '${balanceCurrency}', ${version}, now(), now())`;
}

interface TransactionValues {
  id: string;
  walletId: string;
  idempotencyKey: string;
  playerId?: string;
  providerId?: string;
  externalTransactionId?: string;
  kind?: string;
  status?: string;
  referenceTransactionId?: string | null;
}

function transactionSql(values: TransactionValues): string {
  const providerId = values.providerId ?? 'provider-default';
  const externalTransactionId = values.externalTransactionId ?? randomUuid();
  const kind = values.kind ?? 'BET';
  const status = values.status ?? 'PENDING';
  const referenceTransactionId =
    values.referenceTransactionId === undefined || values.referenceTransactionId === null
      ? 'NULL'
      : `'${values.referenceTransactionId}'`;
  return `INSERT INTO wager_transaction (
      id, provider_id, external_transaction_id, idempotency_key, payload_hash,
      wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
      reference_external_transaction_id, reference_transaction_id, status, created_at
    ) VALUES (
      '${values.id}', '${providerId}', '${externalTransactionId}', '${values.idempotencyKey}', '${randomUuid()}',
      '${values.walletId}', '${values.playerId ?? randomUuid()}', 'round-1', 'game-1', '${kind}', '10.00', 'BRL',
      NULL, ${referenceTransactionId}, '${status}', now()
    )`;
}

interface LedgerValues {
  id: string;
  walletId: string;
  transactionId: string;
  direction?: string;
  amount?: string;
  currency?: string;
  balanceBefore?: string;
  balanceBeforeCurrency?: string;
  balanceAfter?: string;
  balanceAfterCurrency?: string;
}

function ledgerSql(values: LedgerValues): string {
  const direction = values.direction ?? 'DEBIT';
  const amount = values.amount ?? '5.00';
  const currency = values.currency ?? 'BRL';
  const balanceBefore = values.balanceBefore ?? '10.00';
  const balanceBeforeCurrency = values.balanceBeforeCurrency ?? 'BRL';
  const balanceAfter = values.balanceAfter ?? '5.00';
  const balanceAfterCurrency = values.balanceAfterCurrency ?? 'BRL';
  return `INSERT INTO wallet_ledger_entry (
      id, wallet_id, transaction_id, direction, amount, currency,
      balance_before_amount, balance_before_currency, balance_after_amount, balance_after_currency, created_at
    ) VALUES (
      '${values.id}', '${values.walletId}', '${values.transactionId}', '${direction}', '${amount}', '${currency}',
      '${balanceBefore}', '${balanceBeforeCurrency}', '${balanceAfter}', '${balanceAfterCurrency}', now()
    )`;
}

interface InboxValues {
  id: string;
  consumerName: string;
  messageId: string;
}

function inboxSql(values: InboxValues): string {
  return `INSERT INTO inbox_message (id, consumer_name, message_id, payload_hash, received_at)
    VALUES ('${values.id}', '${values.consumerName}', '${values.messageId}', '${randomUuid()}', now())`;
}

interface OutboxValues {
  id: string;
  eventId: string;
  /** `null` força violação de NOT NULL; omitido usa o default `now()`. */
  nextAttemptAt?: string | null;
}

function outboxSql(values: OutboxValues): string {
  const nextAttemptAt = values.nextAttemptAt === null ? 'NULL' : (values.nextAttemptAt ?? 'now()');
  return `INSERT INTO outbox_message (id, event_id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at)
    VALUES ('${values.id}', '${values.eventId}', '${randomUuid()}', 'WagerTransactionProcessed', '{}', now(), 0, ${nextAttemptAt})`;
}

// ---- helpers assíncronos que garantem as linhas-pai (FKs) ----

async function insertWallet(values: Partial<WalletValues> = {}): Promise<string> {
  const id = values.id ?? randomUuid();
  await execute(walletSql({ ...values, id, playerId: values.playerId ?? randomUuid() }));
  return id;
}

interface TransactionInsert {
  id?: string;
  walletId?: string;
  idempotencyKey?: string;
  playerId?: string;
  providerId?: string;
  externalTransactionId?: string;
  kind?: string;
  status?: string;
  referenceTransactionId?: string | null;
}

/** Garante uma wallet existente quando `walletId` não é informado. */
async function insertTransaction(
  options: TransactionInsert = {},
): Promise<{ id: string; walletId: string }> {
  const id = options.id ?? randomUuid();
  const walletId = options.walletId ?? (await insertWallet());
  await execute(
    transactionSql({
      ...options,
      id,
      walletId,
      idempotencyKey: options.idempotencyKey ?? randomUuid(),
    }),
  );
  return { id, walletId };
}

interface LedgerInsert {
  id?: string;
  walletId?: string;
  transactionId?: string;
  direction?: string;
  amount?: string;
  currency?: string;
  balanceBefore?: string;
  balanceBeforeCurrency?: string;
  balanceAfter?: string;
  balanceAfterCurrency?: string;
}

/** Garante wallet + transação existentes quando os ids não são informados. */
async function insertLedger(
  options: LedgerInsert = {},
): Promise<{ id: string; walletId: string; transactionId: string }> {
  const id = options.id ?? randomUuid();

  if (options.transactionId !== undefined) {
    const walletId = options.walletId ?? (await insertWallet());
    await execute(
      ledgerSql({ ...options, id, walletId, transactionId: options.transactionId }),
    );
    return { id, walletId, transactionId: options.transactionId };
  }

  const walletId = options.walletId ?? (await insertWallet());
  const { id: transactionId } = await insertTransaction({ walletId });
  await execute(ledgerSql({ ...options, id, walletId, transactionId }));
  return { id, walletId, transactionId };
}

beforeAll(async () => {
  await ensureTestDatabase();
  orm = await MikroORM.init(buildMikroOrmConfig());
  await orm.migrator.up();
});

afterAll(async () => {
  // Defensivo: garante o schema revertido mesmo se um teste falhar (idempotente).
  await orm.migrator.down();
  await orm.close(true);
});

describe('migration: schema aplicado', () => {
  test('sobe as 5 tabelas', async () => {
    const rows = (await execute(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
    )) as Array<{ tablename: string }>;
    const names = rows.map((row) => row.tablename);

    for (const table of TABLES) {
      expect(names).toContain(table);
    }
  });

  test('cria as constraints e índices nomeados do §3', async () => {
    const constraints = (await execute(
      `SELECT conname FROM pg_constraint`,
    )) as Array<{ conname: string }>;
    const constraintNames = constraints.map((row) => row.conname);

    const expectedConstraints = [
      'uq_wallet_player_currency',
      'ck_wallet_balance_nonneg',
      'ck_wallet_version_pos',
      'ck_wallet_balance_currency',
      'uq_wager_idempotency',
      'uq_wager_provider_external',
      'uq_ledger_tx_wallet',
      'ck_ledger_amount_pos',
      'ck_ledger_balanced',
      'ck_ledger_same_currency',
      'uq_inbox_consumer_msg',
      'uq_outbox_event',
      'fk_wager_wallet',
      'fk_ledger_wallet',
      'fk_ledger_transaction',
      'fk_wager_reference',
    ];
    for (const name of expectedConstraints) {
      expect(constraintNames).toContain(name);
    }

    const indexes = (await execute(
      `SELECT indexname FROM pg_indexes WHERE schemaname = 'public'`,
    )) as Array<{ indexname: string }>;
    const indexNames = indexes.map((row) => row.indexname);
    const expectedIndexes = [
      'uq_wager_single_reversal',
      'ix_wager_wallet_status',
      'ix_wager_reference',
      'ix_ledger_wallet_id',
      'ix_outbox_pending',
      'ix_wager_pending_ref',
    ];
    for (const name of expectedIndexes) {
      expect(indexNames).toContain(name);
    }
  });

  test('cria as funções/triggers de imutabilidade', async () => {
    const functions = (await execute(
      `SELECT proname FROM pg_proc WHERE proname IN ('trg_ledger_immutable', 'trg_wager_terminal_immutable')`,
    )) as Array<{ proname: string }>;
    expect(functions).toHaveLength(2);

    const triggers = (await execute(
      `SELECT tgname FROM pg_trigger WHERE tgname IN ('trg_ledger_immutable', 'trg_wager_terminal_immutable')`,
    )) as Array<{ tgname: string }>;
    expect(triggers).toHaveLength(2);
  });
});

describe('constraints de unicidade', () => {
  test('uq_wallet_player_currency: rejeita player+currency duplicado', async () => {
    const playerId = randomUuid();

    await insertWallet({ playerId, balance: '10.00' });
    await expectFailure(walletSql({ id: randomUuid(), playerId, balance: '10.00' }), {
      code: '23505',
      constraint: 'uq_wallet_player_currency',
    });
  });

  test('uq_wager_idempotency: rejeita idempotency_key duplicada', async () => {
    const idempotencyKey = randomUuid();

    await insertTransaction({ idempotencyKey });
    const walletId = await insertWallet();
    await expectFailure(transactionSql({ id: randomUuid(), walletId, idempotencyKey }), {
      code: '23505',
      constraint: 'uq_wager_idempotency',
    });
  });

  test('uq_wager_provider_external: rejeita (provider_id, external_transaction_id) duplicado', async () => {
    const providerId = `provider-${randomUuid()}`;
    const externalTransactionId = randomUuid();

    await insertTransaction({ providerId, externalTransactionId });
    const walletId = await insertWallet();
    await expectFailure(
      transactionSql({
        id: randomUuid(),
        walletId,
        idempotencyKey: randomUuid(),
        providerId,
        externalTransactionId,
      }),
      { code: '23505', constraint: 'uq_wager_provider_external' },
    );
  });

  test('uq_inbox_consumer_msg: rejeita (consumer_name, message_id) duplicado', async () => {
    const consumerName = 'wager-sqs-consumer';
    const messageId = randomUuid();

    await execute(inboxSql({ id: randomUuid(), consumerName, messageId }));
    await expectFailure(inboxSql({ id: randomUuid(), consumerName, messageId }), {
      code: '23505',
      constraint: 'uq_inbox_consumer_msg',
    });
  });

  test('uq_outbox_event: rejeita event_id duplicado', async () => {
    const eventId = randomUuid();

    await execute(outboxSql({ id: randomUuid(), eventId }));
    await expectFailure(outboxSql({ id: randomUuid(), eventId }), {
      code: '23505',
      constraint: 'uq_outbox_event',
    });
  });

  test('uq_ledger_tx_wallet: rejeita 2 lançamentos para a mesma (transaction_id, wallet_id)', async () => {
    const { id: transactionId, walletId } = await insertTransaction();

    await execute(ledgerSql({ id: randomUuid(), walletId, transactionId }));
    await expectFailure(ledgerSql({ id: randomUuid(), walletId, transactionId }), {
      code: '23505',
      constraint: 'uq_ledger_tx_wallet',
    });
  });
});

describe('constraints de não-negatividade, versão e moeda', () => {
  test('ck_wallet_balance_nonneg: rejeita saldo negativo', async () => {
    await expectFailure(walletSql({ id: randomUuid(), playerId: randomUuid(), balance: '-0.01' }), {
      code: '23514',
      constraint: 'ck_wallet_balance_nonneg',
    });
  });

  test('ck_wallet_version_pos: rejeita version < 1', async () => {
    await expectFailure(
      walletSql({ id: randomUuid(), playerId: randomUuid(), version: 0 }),
      { code: '23514', constraint: 'ck_wallet_version_pos' },
    );
  });

  test('ck_wallet_version_pos: aceita version = 1', async () => {
    await insertWallet({ version: 1, balance: '0.00' });
  });

  test('ck_wallet_balance_currency: rejeita balance_currency != currency', async () => {
    await expectFailure(
      walletSql({
        id: randomUuid(),
        playerId: randomUuid(),
        currency: 'BRL',
        balanceCurrency: 'USD',
      }),
      { code: '23514', constraint: 'ck_wallet_balance_currency' },
    );
  });
});

describe('FKs: integridade referencial', () => {
  test('fk_wager_wallet: rejeita transação com wallet inexistente', async () => {
    await expectFailure(
      transactionSql({
        id: randomUuid(),
        walletId: randomUuid(),
        idempotencyKey: randomUuid(),
      }),
      { code: '23503', constraint: 'fk_wager_wallet' },
    );
  });

  test('fk_ledger_wallet: rejeita lançamento com wallet inexistente', async () => {
    const { id: transactionId } = await insertTransaction();

    await expectFailure(
      ledgerSql({ id: randomUuid(), walletId: randomUuid(), transactionId }),
      { code: '23503', constraint: 'fk_ledger_wallet' },
    );
  });

  test('fk_ledger_transaction: rejeita lançamento com transação inexistente', async () => {
    const walletId = await insertWallet();

    await expectFailure(ledgerSql({ id: randomUuid(), walletId, transactionId: randomUuid() }), {
      code: '23503',
      constraint: 'fk_ledger_transaction',
    });
  });

  test('fk_wager_reference: rejeita referência a transação inexistente', async () => {
    const walletId = await insertWallet();

    await expectFailure(
      transactionSql({
        id: randomUuid(),
        walletId,
        idempotencyKey: randomUuid(),
        referenceTransactionId: randomUuid(),
      }),
      { code: '23503', constraint: 'fk_wager_reference' },
    );
  });
});

describe('ledger: aritmética e moeda', () => {
  test('ck_ledger_amount_pos: rejeita amount <= 0', async () => {
    const { id: transactionId, walletId } = await insertTransaction();

    await expectFailure(
      ledgerSql({
        id: randomUuid(),
        walletId,
        transactionId,
        direction: 'DEBIT',
        amount: '0.00',
        balanceBefore: '10.00',
        balanceAfter: '10.00',
      }),
      { code: '23514', constraint: 'ck_ledger_amount_pos' },
    );
  });

  test('ck_ledger_balanced: rejeita débito desbalanceado', async () => {
    const { id: transactionId, walletId } = await insertTransaction();

    await expectFailure(
      ledgerSql({
        id: randomUuid(),
        walletId,
        transactionId,
        direction: 'DEBIT',
        amount: '3.00',
        balanceBefore: '10.00',
        balanceAfter: '5.00',
      }),
      { code: '23514', constraint: 'ck_ledger_balanced' },
    );
  });

  test('ck_ledger_balanced: aceita crédito balanceado', async () => {
    await insertLedger({
      direction: 'CREDIT',
      amount: '5.00',
      balanceBefore: '10.00',
      balanceAfter: '15.00',
    });
  });

  test('ck_ledger_same_currency: rejeita moedas divergentes', async () => {
    const { id: transactionId, walletId } = await insertTransaction();

    await expectFailure(
      ledgerSql({
        id: randomUuid(),
        walletId,
        transactionId,
        direction: 'DEBIT',
        amount: '5.00',
        currency: 'USD',
        balanceBefore: '10.00',
        balanceBeforeCurrency: 'BRL',
        balanceAfter: '5.00',
        balanceAfterCurrency: 'BRL',
      }),
      { code: '23514', constraint: 'ck_ledger_same_currency' },
    );
  });
});

describe('ledger: imutabilidade (trigger)', () => {
  test('trg_ledger_immutable: bloqueia UPDATE', async () => {
    const { id } = await insertLedger();

    await expectFailure(`UPDATE wallet_ledger_entry SET amount = '6.00' WHERE id = '${id}'`, {
      code: '23001',
      message: 'is immutable',
    });
  });

  test('trg_ledger_immutable: bloqueia DELETE', async () => {
    const { id } = await insertLedger();

    await expectFailure(`DELETE FROM wallet_ledger_entry WHERE id = '${id}'`, {
      code: '23001',
      message: 'is immutable',
    });
  });
});

describe('wager_transaction: imutabilidade de linha terminal', () => {
  test('trg_wager_terminal_immutable: bloqueia UPDATE de PROCESSED', async () => {
    const { id } = await insertTransaction({ status: 'PROCESSED' });

    await expectFailure(`UPDATE wager_transaction SET status = 'REJECTED' WHERE id = '${id}'`, {
      code: '23001',
      message: 'is terminal',
    });
  });

  test('trg_wager_terminal_immutable: bloqueia UPDATE de REJECTED e FAILED', async () => {
    for (const status of ['REJECTED', 'FAILED']) {
      const { id } = await insertTransaction({ status });
      await expectFailure(`UPDATE wager_transaction SET status = 'PENDING' WHERE id = '${id}'`, {
        code: '23001',
        message: 'is terminal',
      });
    }
  });
});

describe('wager_transaction: transições de linha não-terminal passam', () => {
  test('PENDING → PROCESSED', async () => {
    const { id } = await insertTransaction({ status: 'PENDING' });

    await execute(`UPDATE wager_transaction SET status = 'PROCESSED' WHERE id = '${id}'`);
  });

  test('PENDING_REFERENCE → PROCESSED', async () => {
    const { id } = await insertTransaction({ status: 'PENDING_REFERENCE' });

    await execute(`UPDATE wager_transaction SET status = 'PROCESSED' WHERE id = '${id}'`);
  });

  test('PENDING_REFERENCE → REJECTED', async () => {
    const { id } = await insertTransaction({ status: 'PENDING_REFERENCE' });

    await execute(`UPDATE wager_transaction SET status = 'REJECTED' WHERE id = '${id}'`);
  });

  test('permite transicionar uma linha não-terminal', async () => {
    const { id } = await insertTransaction({ status: 'PENDING' });

    await execute(`UPDATE wager_transaction SET status = 'PENDING_REFERENCE' WHERE id = '${id}'`);
  });
});

describe('uq_wager_single_reversal: reversão única por referência', () => {
  test('rejeita segunda reversão PROCESSED (REFUND + ROLLBACK) da mesma referência', async () => {
    const reference = await insertTransaction({ kind: 'BET', status: 'PROCESSED' });
    await insertTransaction({
      kind: 'REFUND',
      status: 'PROCESSED',
      referenceTransactionId: reference.id,
    });

    const walletId = await insertWallet();
    await expectFailure(
      transactionSql({
        id: randomUuid(),
        walletId,
        idempotencyKey: randomUuid(),
        kind: 'ROLLBACK',
        status: 'PROCESSED',
        referenceTransactionId: reference.id,
      }),
      { code: '23505', constraint: 'uq_wager_single_reversal' },
    );
  });

  test('não conflita com transações não-reversão apontando a mesma referência', async () => {
    const reference = await insertTransaction({ kind: 'BET', status: 'PROCESSED' });

    await insertTransaction({
      kind: 'REFUND',
      status: 'PROCESSED',
      referenceTransactionId: reference.id,
    });
    await insertTransaction({
      kind: 'WIN',
      status: 'PROCESSED',
      referenceTransactionId: reference.id,
    });
  });

  test('não conflita quando a reversão ainda não está PROCESSED', async () => {
    const reference = await insertTransaction({ kind: 'BET', status: 'PROCESSED' });

    await insertTransaction({
      kind: 'REFUND',
      status: 'PENDING_REFERENCE',
      referenceTransactionId: reference.id,
    });
    await insertTransaction({
      kind: 'REFUND',
      status: 'PENDING_REFERENCE',
      referenceTransactionId: reference.id,
    });
  });
});

describe('outbox: next_attempt_at nunca nulo', () => {
  test('rejeita INSERT com next_attempt_at NULL', async () => {
    await expectFailure(
      outboxSql({ id: randomUuid(), eventId: randomUuid(), nextAttemptAt: null }),
      { code: '23502', column: 'next_attempt_at' },
    );
  });

  test('aplica o default now() quando omitido', async () => {
    const eventId = randomUuid();
    await execute(outboxSql({ id: randomUuid(), eventId }));

    const rows = (await execute(
      `SELECT next_attempt_at FROM outbox_message WHERE event_id = '${eventId}'`,
    )) as Array<{ next_attempt_at: Date | string | null }>;
    expect(rows[0]?.next_attempt_at).not.toBeNull();
  });
});

describe('migration: down() reverte tudo', () => {
  test('remove as tabelas, as funções de trigger e as FKs', async () => {
    await orm.migrator.down();

    const tables = (await execute(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY(ARRAY['wallet','wager_transaction','wallet_ledger_entry','inbox_message','outbox_message'])`,
    )) as Array<{ tablename: string }>;
    expect(tables).toHaveLength(0);

    const functions = (await execute(
      `SELECT proname FROM pg_proc WHERE proname IN ('trg_ledger_immutable', 'trg_wager_terminal_immutable')`,
    )) as Array<{ proname: string }>;
    expect(functions).toHaveLength(0);

    const foreignKeys = (await execute(
      `SELECT conname FROM pg_constraint WHERE contype = 'f' AND conname IN ('fk_wager_wallet','fk_ledger_wallet','fk_ledger_transaction','fk_wager_reference')`,
    )) as Array<{ conname: string }>;
    expect(foreignKeys).toHaveLength(0);
  });
});
