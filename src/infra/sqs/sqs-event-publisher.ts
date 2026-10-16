import {
  GetQueueUrlCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';

import type { EventPublisherPort, PublishRequest } from '../../application/ports';

/**
 * Publica o envelope na fila FIFO de eventos (CHALLENGE §11):
 * `MessageGroupId = aggregateId` (ordenação por wallet) e
 * `MessageDeduplicationId = eventId` (duplicata segura para o consumidor).
 * Endpoint/credenciais vêm do env (`AWS_ENDPOINT_URL` etc. — LocalStack).
 */
export class SqsEventPublisher implements EventPublisherPort {
  /** URL resolvida uma vez via `GetQueueUrl`; reconnect após erro refaz a resolução. */
  private queueUrlPromise?: Promise<string>;

  constructor(
    private readonly client: SQSClient,
    private readonly queueName: string,
  ) {}

  async publish(request: PublishRequest): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.resolveQueueUrl(),
        MessageBody: request.payload,
        MessageGroupId: request.aggregateId,
        MessageDeduplicationId: request.eventId,
      }),
    );
  }

  private resolveQueueUrl(): Promise<string> {
    this.queueUrlPromise ??= this.client
      .send(new GetQueueUrlCommand({ QueueName: this.queueName }))
      .then((response) => {
        if (!response.QueueUrl) {
          throw new Error(`SQS queue "${this.queueName}" não tem QueueUrl`);
        }
        return response.QueueUrl;
      })
      .catch((error) => {
        // Falha transitória da resolução não é memorizada.
        this.queueUrlPromise = undefined;
        throw error;
      });
    return this.queueUrlPromise;
  }
}
