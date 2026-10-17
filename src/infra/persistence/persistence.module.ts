import { MikroORM } from '@mikro-orm/postgresql';
import { Global, Module } from '@nestjs/common';

import { METRICS, UNIT_OF_WORK, WALLET_READ_STORE } from '../../application/ports';
import type { MetricsPort } from '../../application/ports';
import { loadConfig } from '../config/env';
import { MikroOrmUnitOfWork, UNIT_OF_WORK_OPTIONS } from './unit-of-work';
import { MikroOrmWalletReadStore } from './wallet-read.store';

/**
 * Liga a porta `UnitOfWork` à implementação MikroORM (mesmo `EntityManager`
 * para toda a execução) e a porta de leitura `WalletReadStore` (sem UoW).
 * Global para que os módulos da `api/` só dependam de `application`
 * (sem importar `infra/`).
 */
@Global()
@Module({
  providers: [
    {
      provide: UNIT_OF_WORK_OPTIONS,
      useFactory: (metrics: MetricsPort) => {
        const { database } = loadConfig();
        return { lockTimeoutMs: database.lockTimeoutMs, metrics };
      },
      inject: [METRICS],
    },
    { provide: UNIT_OF_WORK, useClass: MikroOrmUnitOfWork },
    {
      provide: WALLET_READ_STORE,
      useFactory: (orm: MikroORM) => new MikroOrmWalletReadStore(orm),
      inject: [MikroORM],
    },
  ],
  exports: [UNIT_OF_WORK, WALLET_READ_STORE],
})
export class PersistenceModule {}
