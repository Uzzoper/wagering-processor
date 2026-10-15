import { MikroOrmModule } from '@mikro-orm/nestjs';
import { Module } from '@nestjs/common';

import { HealthModule } from './api/health/health.module';
import { DatabaseConnection } from './infra/database/database-connection.service';
import { buildMikroOrmConfig } from './infra/mikro-orm.config';

/**
 * Composition root: liga infraestrutura (MikroORM) e a camada HTTP.
 * As regras de dependência entre domain/application/infra/api estão em ARCHITECTURE.md §1.
 */
@Module({
  imports: [MikroOrmModule.forRoot(buildMikroOrmConfig()), HealthModule],
  providers: [DatabaseConnection],
})
export class AppModule {}
