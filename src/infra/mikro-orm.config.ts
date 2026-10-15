import { defineConfig } from '@mikro-orm/postgresql';

import { loadConfig } from './config/env';

/**
 * Configuração do MikroORM (driver PostgreSQL) derivada de variáveis de ambiente.
 *
 * Sem entidades ainda: o esqueleto não define modelo de domínio persistido.
 * As migrations ficam em `src/infra/migrations` e ainda não existem.
 */
export function buildMikroOrmConfig(env: NodeJS.ProcessEnv = process.env) {
  const { database } = loadConfig(env);

  return defineConfig({
    host: database.host,
    port: database.port,
    dbName: database.name,
    user: database.user,
    password: database.password,
    entities: [],
    entitiesTs: [],
    // Esqueleto ainda não tem entidades; o MikroORM v7 lança se `warnWhenNoEntities` for true.
    discovery: { warnWhenNoEntities: false },
    migrations: {
      path: 'src/infra/migrations',
      pathTs: 'src/infra/migrations',
      glob: '!(*.d).{js,ts}',
      transactional: true,
      allOrNothing: true,
    },
  });
}
