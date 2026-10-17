import { MikroORM } from '@mikro-orm/postgresql';

import { buildMikroOrmConfig } from '../src/infra/mikro-orm.config';

/**
 * Preload de testes do Bun (`bunfig.toml` → `[test] preload`).
 *
 * Isola os testes do banco de desenvolvimento do `.env`: força
 * `DB_NAME=wagering_test` e usa os defaults do docker-compose para
 * host/porta/usuário/senha quando não informados e silencia o logger
 * (`LOG_LEVEL` setada explicitamente vence).
 */
process.env.DB_NAME = 'wagering_test';
process.env.DB_HOST ??= 'localhost';
process.env.DB_PORT ??= '5433';
process.env.DB_USER ??= 'wagering';
process.env.DB_PASSWORD ??= 'wagering';
// Nenhum teste roda o publisher em loop automaticamente: os testes da fatia 7a
// instanciam o `OutboxPublisherWorker` direto, com configuração explícita.
process.env.WORKERS_ENABLED = 'api';
// Testes não imprimem nem leem logs por padrão: `LOG_LEVEL` setada no env vence
// (a suíte de logs `spawnApp` com `LOG_LEVEL=info`).

export const TEST_DB_NAME = process.env.DB_NAME;
const MAINTENANCE_DB_NAME = 'wagering';

/**
 * Chave fixa do advisory lock que serializa a preparação do banco de teste
 * entre os workers paralelos do `bun test`.
 */
const MIGRATION_LOCK_KEY = 762_015_000;

function maintenanceConfig() {
  return buildMikroOrmConfig({ ...process.env, DB_NAME: MAINTENANCE_DB_NAME });
}

/**
 * Recria um banco do zero (`DROP ... WITH (FORCE)` + `CREATE`). Usado pelos
 * testes que precisam de um schema limpo e isolado.
 */
export async function recreateDatabase(name: string): Promise<void> {
  const admin = await MikroORM.init(maintenanceConfig());
  try {
    const connection = admin.em.getConnection();
    await connection.execute(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await connection.execute(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.close(true);
  }
}

/** Remove um banco por completo (`DROP DATABASE ... WITH (FORCE)`). */
export async function dropDatabase(name: string): Promise<void> {
  const admin = await MikroORM.init(maintenanceConfig());
  try {
    await admin.em
      .getConnection()
      .execute(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
  } finally {
    await admin.close(true);
  }
}

/**
 * Garante `wagering_test` criado e migrado UMA vez por run.
 *
 * Roda no preload de cada worker, que executam em paralelo: `pg_advisory_lock`
 * numa chave fixa serializa; o primeiro worker cria o banco e aplica `up`, os
 * demais veem a migration já aplicada (no-op). O lock é liberado no `finally`.
 */
async function prepareTestDatabase(): Promise<void> {
  const admin = await MikroORM.init(maintenanceConfig());

  try {
    const connection = admin.em.getConnection();
    await connection.execute('SELECT pg_advisory_lock(?)', [MIGRATION_LOCK_KEY]);

    try {
      const existing = await connection.execute(
        'SELECT 1 FROM pg_database WHERE datname = ?',
        [TEST_DB_NAME],
      );

      if (existing.length === 0) {
        try {
          await connection.execute(`CREATE DATABASE "${TEST_DB_NAME}"`);
        } catch (error) {
          // PostgreSQL não tem CREATE DATABASE IF NOT EXISTS; corrida é tolerada.
          if ((error as { code?: string }).code !== '42P04') {
            throw error;
          }
        }
      }

      const orm = await MikroORM.init(buildMikroOrmConfig());
      try {
        await orm.migrator.up();
      } finally {
        await orm.close(true);
      }
    } finally {
      await connection.execute('SELECT pg_advisory_unlock(?)', [MIGRATION_LOCK_KEY]);
    }
  } finally {
    await admin.close(true);
  }
}

await prepareTestDatabase();
