import { MikroORM } from '@mikro-orm/postgresql';
import { Global, Module } from '@nestjs/common';

import { UNIT_OF_WORK, WALLET_READ_STORE } from '../../application/ports';
import { MikroOrmUnitOfWork } from './unit-of-work';
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
