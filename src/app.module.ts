import { MikroOrmModule } from '@mikro-orm/nestjs';
import { Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';

import { FailureExceptionFilter } from './api/filters/failure-exception.filter';
import { HealthModule } from './api/health/health.module';
import { WageringModule } from './api/wagering/wagering.module';
import { WalletModule } from './api/wallets/wallet.module';
import { AuthGuard } from './api/auth/auth.guard';
import { DatabaseConnection } from './infra/database/database-connection.service';
import { buildMikroOrmConfig } from './infra/mikro-orm.config';
import { PersistenceModule } from './infra/persistence/persistence.module';
import { MetricsModule } from './infra/observability/metrics.module';
import { SqsModule } from './infra/sqs/sqs.module';

/**
 * Composition root: liga infraestrutura (MikroORM, SQS/workers) e a camada HTTP.
 * As regras de dependência entre domain/application/infra/api estão em ARCHITECTURE.md §1.
 */
@Module({
  imports: [
    MikroOrmModule.forRoot(buildMikroOrmConfig()),
    PersistenceModule,
    MetricsModule,
    SqsModule,
    HealthModule,
    WalletModule,
    WageringModule,
  ],
  providers: [
    DatabaseConnection,
    // Filtro único no módulo raiz: vale também para apps criados direto nos testes.
    { provide: APP_FILTER, useClass: FailureExceptionFilter },
    { provide: APP_GUARD, useClass: AuthGuard },
  ],
})
export class AppModule {}
