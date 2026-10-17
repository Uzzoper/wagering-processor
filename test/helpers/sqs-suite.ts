import {
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

/**
 * Filas privadas FIFO e sondagem de fila para as suítes de multiprocessing
 * (fatia 7d): cada suíte cria as SUAS filas — nunca toca nas de produção
 * nem nas de outra suíte.
 */

export function makeSqsClient(): SQSClient {
  return new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}

/**
 * DLQ privada + fila transacional FIFO com RedrivePolicy (maxReceiveCount 5,
 * mesma forma da produção). `visibilityTimeoutSeconds` curto acelera os
 * redeliveries dos testes de crash recovery.
 */
export async function createPrivateWagerQueues(
  client: SQSClient,
  suiteQueueName: string,
  opts: { visibilityTimeoutSeconds?: number } = {},
): Promise<{ wagerUrl: string; dlqUrl: string }> {
  const dlq = await client.send(
    new CreateQueueCommand({
      QueueName: `${suiteQueueName}-dlq.fifo`,
      Attributes: { FifoQueue: 'true' },
    }),
  );
  const dlqUrl = dlq.QueueUrl!;
  const attrs = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }),
  );
  const redrive = JSON.stringify({
    deadLetterTargetArn: attrs.Attributes!.QueueArn!,
    maxReceiveCount: '5',
  });
  const main = await client.send(
    new CreateQueueCommand({
      QueueName: `${suiteQueueName}.fifo`,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'true',
        VisibilityTimeout: String(opts.visibilityTimeoutSeconds ?? 30),
        RedrivePolicy: redrive,
      },
    }),
  );
  return { wagerUrl: main.QueueUrl!, dlqUrl };
}

/** Fila de eventos da outbox privada (FIFO com dedup por conteúdo). */
export async function createPrivateEventsQueue(
  client: SQSClient,
  suiteQueueName: string,
): Promise<string> {
  const queue = await client.send(
    new CreateQueueCommand({
      QueueName: `${suiteQueueName}-events.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'true' },
    }),
  );
  return queue.QueueUrl!;
}

/**
 * Sondagem da fila: recebe e devolve; por padrão VisibilityTimeout 0 para
 * nunca esconder mensagens da prova final.
 */
export async function pollReceive(
  client: SQSClient,
  queueUrl: string,
  opts: { maxMessages?: number; visibilityTimeout?: number; waitSeconds?: number } = {},
): Promise<Array<{ body: string; receiptHandle: string; attributes: Record<string, string> | undefined }>> {
  const response = await client.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: opts.maxMessages ?? 10,
      WaitTimeSeconds: opts.waitSeconds ?? 1,
      VisibilityTimeout: opts.visibilityTimeout ?? 0,
      AttributeNames: ['All'],
    }),
  );
  return (response.Messages ?? []).map((message) => ({
    body: message.Body ?? '',
    receiptHandle: message.ReceiptHandle!,
    attributes: message.Attributes as Record<string, string> | undefined,
  }));
}

/** Envelope da outbox na fila (evento de integração serializado). */
export interface QueuedEvent {
  eventId: string;
  eventType: string;
  aggregateId: string;
}

/** Drena a fila de eventos: devolve TODOS os envelopes parseados. */
export async function drainEventQueue(
  client: SQSClient,
  queueUrl: string,
  maxRounds = 40,
): Promise<QueuedEvent[]> {
  const parsed: QueuedEvent[] = [];
  for (let round = 0; round < maxRounds; round += 1) {
    const messages = await pollReceive(client, queueUrl, { waitSeconds: 1 });
    if (messages.length === 0) {
      return parsed;
    }
    for (const { body, receiptHandle } of messages) {
      await client.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }),
      );
      try {
        const envelope = JSON.parse(body) as QueuedEvent;
        if (typeof envelope.eventId === 'string') {
          parsed.push(envelope);
        }
      } catch {
        // corpo que não é evento nosso: ignorado (drenado e deletado)
      }
    }
  }
  return parsed;
}
