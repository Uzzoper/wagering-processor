import { Module } from '@nestjs/common';
import { SQSClient } from '@aws-sdk/client-sqs';

import { SQS_CLIENT_TOKEN, SqsModule } from '../../infra/sqs/sqs.module';
import { loadConfig } from '../../infra/config/env';
import { HealthController } from './health.controller';

/**
 * Liveness (sem dependências) e readiness (Postgres + SQS). O client SQS é o
 * mesmo do `SqsModule` (um só por processo) e o nome da fila vem do env.
 */
@Module({
  imports: [
    // Resolve o SQS_CLIENT_TOKEN exportado; nenhum worker adicional é criado:
    // os workers continuam ligados apenas pelo `WORKERS_ENABLED` de cada um.
    SqsModule,
  ],
  controllers: [HealthController],
  providers: [
    {
      provide: 'HealthSqsDeps',
      useFactory: (client: SQSClient) => {
        const { sqs } = loadConfig();
        return { sqs: client, wagerQueueName: sqs.wagerQueueName };
      },
      inject: [SQS_CLIENT_TOKEN],
    },
  ],
})
export class HealthModule {}
