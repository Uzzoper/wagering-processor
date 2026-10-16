import { MikroORM } from '@mikro-orm/postgresql';

import { buildMikroOrmConfig } from '../src/infra/mikro-orm.config';

/**
 * Runner de migrations (`bun run migration:up` / `migration:down`).
 * Usa o mesmo `buildMikroOrmConfig` dos testes e da app; Bun carrega `.env`.
 */
async function main(): Promise<void> {
  const direction = process.argv[2] === 'down' ? 'down' : 'up';
  const orm = await MikroORM.init(buildMikroOrmConfig());

  try {
    const migrator = orm.migrator;
    const executed = direction === 'down' ? await migrator.down() : await migrator.up();
    console.log(
      JSON.stringify({
        level: 'info',
        event: `migration.${direction}`,
        migrations: executed.map((migration) => migration.name),
      }),
    );
  } finally {
    await orm.close(true);
  }
}

void main();
