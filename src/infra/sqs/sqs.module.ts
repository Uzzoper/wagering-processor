import { SQSClient } from '@aws-sdk/client-sqs';
import { Module } from '@nestjs/common';

import { EVENT_PUBLISHER, METRICS, PROVIDER_IDENTITY, UNIT_OF_WORK } from '../../application/ports';
import type {
  EventPublisherPort,
  MetricsPort,
  UnitOfWork,
} from '../../application/ports';
import { SubmitWagerTransactionUseCase } from '../../application/submit-wager.use-case';
import { SYSTEM_CLOCK } from '../config/clock';
import { loadConfig } from '../config/env';
import { OutboxPublisherWorker } from '../workers/outbox-publisher';
import { PendingReferenceWorker } from '../workers/pending-reference-worker';
import { InstrumentedSubmitWagerUseCase } from '../observability/submit-metrics';
import { NoopProviderIdentity } from '../auth/noop-provider-identity';
import { SqsEventPublisher } from './sqs-event-publisher';
import { SqsGauges } from './sqs-gauges';
import { SqsWagerConsumer } from './sqs-wager-consumer';

const SQS_CLIENT = Symbol('SqsClient');

/** Token do client SQS compartilhado (health/ready e métricas de gauge). */
export const SQS_CLIENT_TOKEN = SQS_CLIENT;

/**
 * Adaptador SQS + workers (outbox publisher, consumer e worker de pendências).
 * Os workers se auto-ligam em `onModuleInit` quando `WORKERS_ENABLED`
 * contém `outbox`/`sqs`/`pending` (default: `api,outbox,sqs,pending`).
 * As submissões passam pelo decorador instrumentado — contadores/latência
 * convergem no mesmo registry (`MetricsModule`) em todos os caminhos.
 */
@Module({
  providers: [
    { provide: PROVIDER_IDENTITY, useClass: NoopProviderIdentity },
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
      useFactory: (
        unitOfWork: UnitOfWork,
        publisher: EventPublisherPort,
        metrics: MetricsPort,
      ) => {
        const { outbox } = loadConfig();
        return new OutboxPublisherWorker(unitOfWork, publisher, {
          enabled: outbox.enabled,
          pollIntervalMs: outbox.pollIntervalMs,
          batchSize: outbox.batchSize,
          leaseMs: outbox.leaseMs,
          metrics,
        });
      },
      inject: [UNIT_OF_WORK, EVENT_PUBLISHER, METRICS],
    },
    {
      provide: SqsWagerConsumer,
      useFactory: (
        unitOfWork: UnitOfWork,
        client: SQSClient,
        metrics: MetricsPort,
        identity: import('../../application/ports').ProviderIdentityPort,
      ) => {
        const { sqs } = loadConfig();
        return new SqsWagerConsumer(
          new InstrumentedSubmitWagerUseCase(
            new SubmitWagerTransactionUseCase(unitOfWork, identity),
            metrics,
          ),
          client,
          sqs.wagerQueueName,
          { enabled: sqs.consumerEnabled, metrics },
        );
      },
      inject: [UNIT_OF_WORK, SQS_CLIENT, METRICS, PROVIDER_IDENTITY],
    },
    {
      provide: PendingReferenceWorker,
      useFactory: (unitOfWork: UnitOfWork, metrics: MetricsPort, identity: import('../../application/ports').ProviderIdentityPort) => {
        const { pendingRef } = loadConfig();
        return new PendingReferenceWorker(
          unitOfWork,
          new InstrumentedSubmitWagerUseCase(
            new SubmitWagerTransactionUseCase(unitOfWork, identity),
            metrics,
          ),
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
      inject: [UNIT_OF_WORK, METRICS, PROVIDER_IDENTITY],
    },
    {
      provide: 'SQS_GAUGES_DEPS',
      useFactory: (client: SQSClient, metrics: MetricsPort) => {
        const { sqs } = loadConfig();
        return { sqs: client, wagerQueueName: sqs.wagerQueueName, registry: metrics };
      },
      inject: [SQS_CLIENT, METRICS],
    },
    SqsGauges,
  ],
  exports: [
    SQS_CLIENT_TOKEN,
    EVENT_PUBLISHER,
    OutboxPublisherWorker,
    SqsWagerConsumer,
    PendingReferenceWorker,
  ],
})
export class SqsModule {}
