import { Module } from '@nestjs/common';

import { METRICS, PROVIDER_IDENTITY, UNIT_OF_WORK } from '../../application/ports';
import type { MetricsPort, ProviderIdentityPort, UnitOfWork } from '../../application/ports';
import { GetWagerTransactionUseCase } from '../../application/get-wager.use-case';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { InstrumentedSubmitWagerUseCase } from '../../infra/observability/submit-metrics';
import { NoopProviderIdentity } from '../../infra/auth/noop-provider-identity';
import { WageringController } from './wagering.controller';

/**
 * Use cases puros ligados por fábrica; `UnitOfWork` vem do `PersistenceModule`
 * global. A submissão passa pelo decorador instrumentado (mesmo registry dos
 * caminhos SQS) — contadores de transação, duplicata e latência.
 */
@Module({
  controllers: [WageringController],
  providers: [
    { provide: PROVIDER_IDENTITY, useClass: NoopProviderIdentity },
    {
      provide: SubmitWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork, metrics: MetricsPort, identity: ProviderIdentityPort) =>
        new InstrumentedSubmitWagerUseCase(
          new SubmitWagerTransactionUseCase(unitOfWork, identity),
          metrics,
        ),
      inject: [UNIT_OF_WORK, METRICS, PROVIDER_IDENTITY],
    },
    {
      provide: GetWagerTransactionUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new GetWagerTransactionUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
  ],
})
export class WageringModule {}
