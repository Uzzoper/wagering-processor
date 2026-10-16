import { SQSClient } from '@aws-sdk/client-sqs';
import { Module } from '@nestjs/common';

import { EVENT_PUBLISHER, UNIT_OF_WORK } from '../../application/ports';
import { loadConfig } from '../config/env';
import { OutboxPublisherWorker } from '../workers/outbox-publisher';
import { SqsEventPublisher } from './sqs-event-publisher';

const SQS_CLIENT = Symbol('SqsClient');

/**
 * Adaptador SQS + worker da outbox. O worker se auto-liga em `onModuleInit`
 * quando `WORKERS_ENABLED` contém `outbox` (default: `api,outbox`).
 */
@Module({
  providers: [
    {
      provide: SQS_CLIENT,
      useFactory: () => {
        const { sqs } = loadConfig();
        return new SQSClient({
          region: sqs.region,
          ...(sqs.endpoint ? { endpoint: sqs.endpoint } : {}),
          ...(sqs.accessKeyId
            ? {
                credentials: {
                  accessKeyId: sqs.accessKeyId,
                  secretAccessKey: sqs.secretAccessKey ?? '',
                },
              }
            : {}),
        });
      },
    },
    {
      provide: EVENT_PUBLISHER,
      useFactory: (client: SQSClient) => {
        const { sqs } = loadConfig();
        return new SqsEventPublisher(client, sqs.eventsQueueName);
      },
      inject: [SQS_CLIENT],
    },
    {
      provide: OutboxPublisherWorker,
      useFactory: (unitOfWork, publisher) => {
        const { outbox } = loadConfig();
        return new OutboxPublisherWorker(unitOfWork, publisher, {
          enabled: outbox.enabled,
          pollIntervalMs: outbox.pollIntervalMs,
          batchSize: outbox.batchSize,
          leaseMs: outbox.leaseMs,
        });
      },
      inject: [UNIT_OF_WORK, EVENT_PUBLISHER],
    },
  ],
  exports: [EVENT_PUBLISHER, OutboxPublisherWorker],
})
export class SqsModule {}
