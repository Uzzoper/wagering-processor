import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import { dropDatabase, recreateDatabase } from '../setup';

/**
 * Reversão da migration num banco DEDICADO (`wagering_migration_test`), separado
 * do `wagering_test` dos demais testes. O banco é recriado do zero no início,
 * recebe `up`, é conferido, sofre `down` e no fim é DROPADO por completo.
 */

const MIGRATION_DB_NAME = 'wagering_migration_test';

const TABLES = [
  'wallet',
  'wager_transaction',
  'wallet_ledger_entry',
  'inbox_message',
  'outbox_message',
];

const FOREIGN_KEYS = [
  'fk_wager_wallet',
  'fk_ledger_wallet',
  'fk_ledger_transaction',
  'fk_wager_reference',
];

const tablesArray = `ARRAY[${TABLES.map((table) => `'${table}'`).join(', ')}]`;
const fksArray = `ARRAY[${FOREIGN_KEYS.map((fk) => `'${fk}'`).join(', ')}]`;

let orm: MikroORM;

function execute(sql: string): Promise<unknown[]> {
  return orm.em.getConnection().execute(sql);
}

beforeAll(async () => {
  await recreateDatabase(MIGRATION_DB_NAME);
  orm = await MikroORM.init(
    buildMikroOrmConfig({ ...process.env, DB_NAME: MIGRATION_DB_NAME }),
  );
  await orm.migrator.up();
});

afterAll(async () => {
  // Fecha TODAS as conexões do teste e remove o banco por completo.
  await orm.close(true);
  await dropDatabase(MIGRATION_DB_NAME);
});

describe('migration: down() reverte tudo', () => {
  test('sobe as tabelas e depois remove tabelas, funções de trigger, FKs e o índice keyset', async () => {
    const tablesBefore = (await execute(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY(${tablesArray})`,
    )) as Array<{ tablename: string }>;
    expect(tablesBefore).toHaveLength(TABLES.length);

    // Índice novo da fatia 8a existe ANTES do down (o down() remove).
    const indexesBefore = (await execute(
      `SELECT indexname FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'idx_ledger_wallet_created_id'`,
    )) as Array<{ indexname: string }>;
    expect(indexesBefore).toHaveLength(1);

    // Reverte TODAS as migrations (a segunda viria a falhar: down() sem
    // intervalo só desfaz a última — o índice separado da 8a).
    await orm.migrator.down({ to: 0 });

    const tablesAfter = (await execute(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY(${tablesArray})`,
    )) as Array<{ tablename: string }>;
    expect(tablesAfter).toHaveLength(0);

    const functions = (await execute(
      `SELECT proname FROM pg_proc WHERE proname IN ('trg_ledger_immutable', 'trg_wager_terminal_immutable')`,
    )) as Array<{ proname: string }>;
    expect(functions).toHaveLength(0);

    const foreignKeys = (await execute(
      `SELECT conname FROM pg_constraint WHERE contype = 'f' AND conname = ANY(${fksArray})`,
    )) as Array<{ conname: string }>;
    expect(foreignKeys).toHaveLength(0);

    const indexesAfter = (await execute(
      `SELECT indexname FROM pg_indexes WHERE indexname = 'idx_ledger_wallet_created_id'`,
    )) as Array<{ indexname: string }>;
    expect(indexesAfter).toHaveLength(0);
  });
});
