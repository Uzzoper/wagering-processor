import { Global, Module } from '@nestjs/common';

import { UNIT_OF_WORK } from '../../application/ports';
import { MikroOrmUnitOfWork } from './unit-of-work';

/**
 * Liga a porta `UnitOfWork` à implementação MikroORM. Global para que os
 * módulos da `api/` só dependam de `application` (sem importar `infra/`).
 */
@Global()
@Module({
  providers: [{ provide: UNIT_OF_WORK, useClass: MikroOrmUnitOfWork }],
  exports: [UNIT_OF_WORK],
})
export class PersistenceModule {}
