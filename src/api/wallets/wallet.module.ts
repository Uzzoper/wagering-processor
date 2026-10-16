import { Module } from '@nestjs/common';

import { CreateWalletUseCase } from '../../application/create-wallet.use-case';
import { GetWalletUseCase } from '../../application/get-wallet.use-case';
import type { UnitOfWork } from '../../application/ports';
import { UNIT_OF_WORK } from '../../application/ports';
import { WalletController } from './wallet.controller';

/**
 * Use cases são classes puras (sem decorators) ligadas por fábrica; o
 * `UnitOfWork` vem do `PersistenceModule` global, então a api não importa infra.
 */
@Module({
  controllers: [WalletController],
  providers: [
    {
      provide: CreateWalletUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new CreateWalletUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
    {
      provide: GetWalletUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new GetWalletUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
  ],
})
export class WalletModule {}
