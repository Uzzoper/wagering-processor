import { SQSClient } from '@aws-sdk/client-sqs';
import { Module } from '@nestjs/common';

import { EVENT_PUBLISHER, UNIT_OF_WORK, type UnitOfWork } from '../../application/ports';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { loadConfig } from '../config/env';
import { OutboxPublisherWorker } from '../workers/outbox-publisher';
import { SqsEventPublisher } from './sqs-event-publisher';
import { SqsWagerConsumer } from './sqs-wager-consumer';

const SQS_CLIENT = Symbol('SqsClient');

/**
 * Adaptador SQS + workers (outbox publisher e consumer de transações).
 * Os workers se auto-likam em `onModuleInit` quando `WORKERS_ENABLED`
 * contém `outbox`/`sqs` (default: `api,outbox,sqs`).
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
    {
      provide: SqsWagerConsumer,
      useFactory: (unitOfWork: UnitOfWork, client: SQSClient) => {
        const { sqs } = loadConfig();
        return new SqsWagerConsumer(
          new SubmitWagerTransactionUseCase(unitOfWork),
          client,
          sqs.wagerQueueName,
          { enabled: sqs.consumerEnabled },
        );
      },
      inject: [UNIT_OF_WORK, SQS_CLIENT],
    },
  ],
  exports: [EVENT_PUBLISHER, OutboxPublisherWorker, SqsWagerConsumer],
})
export class SqsModule {}
