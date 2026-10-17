import { MikroORM } from '@mikro-orm/core';
import { Inject, Injectable, type OnApplicationBootstrap } from '@nestjs/common';

import { logEvent } from '../observability/json-logger';

const CONNECT_TIMEOUT_MS = 3_000;

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout de ${ms}ms ao conectar no banco`)), ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

/**
 * Abre a conexão do MikroORM na subida e loga o resultado em JSON (uma linha).
 * A indisponibilidade do banco NÃO derruba o processo: `GET /health/live` não depende do banco.
 */
@Injectable()
export class DatabaseConnection implements OnApplicationBootstrap {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  async onApplicationBootstrap(): Promise<void> {
    const config = this.orm.config;
    const context = {
      driver: 'postgresql',
      host: config.get('host'),
      port: config.get('port'),
      database: config.get('dbName'),
    };

    try {
      await this.orm.connect();
      const check = await withTimeout(this.orm.em.getConnection().checkConnection(), CONNECT_TIMEOUT_MS);

      if (check.ok) {
        logEvent('info', 'database.connected', context);
      } else {
        logEvent('warn', 'database.connect_failed', { reason: check.reason, ...context });
      }
    } catch (error) {
      logEvent('warn', 'database.connect_failed', {
        reason: error instanceof Error ? error.message : String(error),
        ...context,
      });
    }
  }
}
