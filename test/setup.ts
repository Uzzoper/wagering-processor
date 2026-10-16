import { MikroORM } from '@mikro-orm/postgresql';

import { buildMikroOrmConfig } from '../src/infra/mikro-orm.config';

/**
 * Preload de testes do Bun (`bunfig.toml` → `[test] preload`).
 *
 * Isola os testes do banco de desenvolvimento do `.env`: força
 * `DB_NAME=wagering_test` e usa os defaults do docker-compose para
 * host/porta/usuário/senha quando não informados.
 */
process.env.DB_NAME = 'wagering_test';
process.env.DB_HOST ??= 'localhost';
process.env.DB_PORT ??= '5433';
process.env.DB_USER ??= 'wagering';
process.env.DB_PASSWORD ??= 'wagering';

export const TEST_DB_NAME = process.env.DB_NAME;
const MAINTENANCE_DB_NAME = 'wagering';

/**
 * Conecta no banco de manutenção `wagering` e cria `wagering_test` se não existir.
 *
 * PostgreSQL não tem `CREATE DATABASE IF NOT EXISTS`; entre workers do `bun test`
 * a criação pode correr em paralelo, então toleramos o SQLSTATE `42P04`
 * (duplicate_database). É idempotente e roda também no `beforeAll` dos testes
 * de integração.
 */
export async function ensureTestDatabase(): Promise<void> {
  const adminOrm = await MikroORM.init(
    buildMikroOrmConfig({ ...process.env, DB_NAME: MAINTENANCE_DB_NAME }),
  );

  try {
    const connection = adminOrm.em.getConnection();
    const existing = await connection.execute('SELECT 1 FROM pg_database WHERE datname = ?', [
      TEST_DB_NAME,
    ]);

    if (existing.length === 0) {
      try {
        await connection.execute(`CREATE DATABASE "${TEST_DB_NAME}"`);
      } catch (error) {
        if ((error as { code?: string }).code !== '42P04') {
          throw error;
        }
      }
    }
  } finally {
    await adminOrm.close(true);
  }
}

await ensureTestDatabase();
