import { SQSClient } from '@aws-sdk/client-sqs';
import { Module } from '@nestjs/common';

import { EVENT_PUBLISHER, UNIT_OF_WORK, type UnitOfWork } from '../../application/ports';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { SYSTEM_CLOCK } from '../config/clock';
import { loadConfig } from '../config/env';
import { OutboxPublisherWorker } from '../workers/outbox-publisher';
import { PendingReferenceWorker } from '../workers/pending-reference-worker';
import { SqsEventPublisher } from './sqs-event-publisher';
import { SqsWagerConsumer } from './sqs-wager-consumer';

const SQS_CLIENT = Symbol('SqsClient');

/** Token do client SQS compartilhado (health/ready e métricas de gauge). */
export const SQS_CLIENT_TOKEN = SQS_CLIENT;

/**
 * Adaptador SQS + workers (outbox publisher, consumer e worker de pendências).
 * Os workers se auto-ligam em `onModuleInit` quando `WORKERS_ENABLED`
 * contém `outbox`/`sqs`/`pending` (default: `api,outbox,sqs,pending`).
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
    {
      provide: PendingReferenceWorker,
      useFactory: (unitOfWork: UnitOfWork) => {
        const { pendingRef } = loadConfig();
        return new PendingReferenceWorker(
          unitOfWork,
          new SubmitWagerTransactionUseCase(unitOfWork),
          {
            enabled: pendingRef.enabled,
            pollIntervalMs: pendingRef.pollIntervalMs,
            ttlHours: pendingRef.ttlHours,
            maxAttempts: pendingRef.maxAttempts,
            // Clone do clock default: nenhuma instância compartilha a porta.
            clock: { ...SYSTEM_CLOCK },
          },
        );
      },
      inject: [UNIT_OF_WORK],
    },
  ],
  exports: [SQS_CLIENT_TOKEN, EVENT_PUBLISHER, OutboxPublisherWorker, SqsWagerConsumer, PendingReferenceWorker],
})
export class SqsModule {}
