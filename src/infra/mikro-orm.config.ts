import { Migrator } from '@mikro-orm/migrations';
import { defineConfig } from '@mikro-orm/postgresql';

import { loadConfig } from './config/env';
import {
  InboxMessageEntity,
  OutboxMessageEntity,
  WagerTransactionEntity,
  WalletEntity,
  WalletLedgerEntryEntity,
} from './persistence';

/**
 * Configuração do MikroORM (driver PostgreSQL) derivada de variáveis de ambiente.
 *
 * Entidades-espelho finas (só colunas + índices) em `src/infra/persistence`.
 * O schema real é criado pelas migrations em `src/infra/migrations` — nunca por
 * `schema:update`. As regras de negócio vivem no domínio (ARCHITECTURE.md §1).
 */
export function buildMikroOrmConfig(env: NodeJS.ProcessEnv = process.env) {
  const { database } = loadConfig(env);

  const entities = [
    WalletEntity,
    WagerTransactionEntity,
    WalletLedgerEntryEntity,
    InboxMessageEntity,
    OutboxMessageEntity,
  ];

  return defineConfig({
    host: database.host,
    port: database.port,
    dbName: database.name,
    user: database.user,
    password: database.password,
    entities,
    entitiesTs: entities,
    // Registra o Migrator como extensão (MikroORM v7: acesso via `orm.migrator`).
    extensions: [Migrator],
    // O schema é governado por migrations; nada de sync automático.
    migrations: {
      path: 'src/infra/migrations',
      pathTs: 'src/infra/migrations',
      glob: '!(*.d).{js,ts}',
      transactional: true,
      allOrNothing: true,
    },
  });
}
