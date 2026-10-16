import { Module } from '@nestjs/common';

import type { UnitOfWork } from '../../application/ports';
import { UNIT_OF_WORK } from '../../application/ports';
import { GetWagerTransactionUseCase } from '../../application/get-wager.use-case';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { WageringController } from './wagering.controller';

/** Use cases puros ligados por fábrica; `UnitOfWork` vem do `PersistenceModule` global. */
@Module({
  controllers: [WageringController],
  providers: [
    {
      provide: SubmitWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new SubmitWagerTransactionUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
    {
      provide: GetWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new GetWagerTransactionUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
  ],
})
export class WageringModule {}
