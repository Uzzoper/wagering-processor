import { Module } from '@nestjs/common';

import { METRICS, UNIT_OF_WORK } from '../../application/ports';
import type { MetricsPort, UnitOfWork } from '../../application/ports';
import { GetWagerTransactionUseCase } from '../../application/get-wager.use-case';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { InstrumentedSubmitWagerUseCase } from '../../infra/observability/submit-metrics';
import { WageringController } from './wagering.controller';

/**
 * Use cases puros ligados por fábrica; `UnitOfWork` vem do `PersistenceModule`
 * global. A submissão passa pelo decorador instrumentado (mesmo registry dos
 * caminhos SQS) — contadores de transação, duplicata e latência.
 */
@Module({
  controllers: [WageringController],
  providers: [
    {
      provide: SubmitWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork, metrics: MetricsPort) =>
        new InstrumentedSubmitWagerUseCase(
          new SubmitWagerTransactionUseCase(unitOfWork),
          metrics,
        ),
      inject: [UNIT_OF_WORK, METRICS],
    },
    {
      provide: GetWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new GetWagerTransactionUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
  ],
})
export class WageringModule {}
