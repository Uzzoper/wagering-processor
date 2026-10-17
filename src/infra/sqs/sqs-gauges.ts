import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';

import type { MetricsPort } from '../../application/ports';
import { SQS_CLIENT_TOKEN } from './sqs.module';

/** Cache das consultas ao SQS — o gauge é caro relativamente ao poll. */
const DLQ_CACHE_TTL_MS = 5_000;

/** Deps do provider (nome da fila como valor precisa de objeto para DI). */
export interface SqsGaugesDeps {
  sqs: SQSClient;
  wagerQueueName: string;
  registry: MetricsPort;
}

/**
 * Gauges de SQS no scrape (fatia 8b): `sqs_dlq_messages` via GetQueueAttributes
 * da DLQ (default derivado da fila de transações: `<fila>-dlq.fifo`). Erro do
 * SQS omite o gauge no snapshot — readiness cobre o estado do SQS.
 */
@Injectable()
export class SqsGauges implements OnModuleInit {
  private cached?: { at: number; value: number };
  private inFlight?: Promise<number>;

  constructor(@Inject('SQS_GAUGES_DEPS') private readonly deps: SqsGaugesDeps) {}

  onModuleInit(): void {
    this.deps.registry.setGauge('sqs_dlq_messages', () => this.dlqMessages());
  }

  private async dlqMessages(): Promise<number> {
    if (this.cached !== undefined && Date.now() - this.cached.at < DLQ_CACHE_TTL_MS) {
      return this.cached.value;
    }
    this.inFlight ??= this.fetchDlqMessages()
      .then((value) => {
        this.cached = { at: Date.now(), value };
        this.inFlight = undefined;
        return value;
      })
      .catch((error) => {
        this.inFlight = undefined;
        throw error;
      });
    return this.inFlight;
  }

  private async fetchDlqMessages(): Promise<number> {
    const queueName = `${this.deps.wagerQueueName.replace(/\.fifo$/, '')}-dlq.fifo`;
    const queueUrl = await this.resolveQueueUrl(queueName);
    const response = await this.deps.sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      }),
    );
    const count = response.Attributes?.ApproximateNumberOfMessages;
    return count === undefined ? 0 : Number(count);
  }

  private async resolveQueueUrl(queueName: string): Promise<string> {
    // Resolve toda vez: a fila pode ter sido recriada entre scrapes (testes).
    const response = await this.deps.sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
    if (!response.QueueUrl) {
      throw new Error(`SQS queue "${queueName}" não tem QueueUrl`);
    }
    return response.QueueUrl;
  }
}
