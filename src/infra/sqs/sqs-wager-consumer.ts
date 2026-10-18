import { createHash } from 'node:crypto';

import { Injectable, type OnApplicationShutdown, type OnModuleInit } from '@nestjs/common';
import {
  ChangeMessageVisibilityCommand,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  type QueueAttributeName,
  type SQSClient,
} from '@aws-sdk/client-sqs';

import { logEvent } from '../observability/json-logger';
import { withLogContextFields } from '../observability/log-context';
import {
  type SubmitWagerCommands,
  type SubmitWagerInput,
  type SubmitWagerResult,
} from '../../application/submit-wager.use-case';
import { SQS_RETRIES_TOTAL, WAGER_DUPLICATES_TOTAL } from '../../application/metric-names';
import type { MetricsPort } from '../../application/ports';
import { WagerInputError, parseWagerInput, type WagerInputBody } from '../../application/wager-input';
import { InboxMessage } from '../../domain/inbox-message';
import { sqsRetryDelaySeconds } from './sqs-retry-policy';

export const SQS_WAGER_CONSUMER_NAME = 'wager-sqs-consumer';

/** Mensagem crua da SQS SDK — só o que o consumer usa. */
interface SqsMessage {
  MessageId?: string;
  ReceiptHandle?: string;
  Body?: string;
  Attributes?: Record<string, string>;
}

export interface SqsWagerConsumerOptions {
  /** `WORKERS_ENABLED` contém `sqs`. */
  enabled: boolean;
  /** Consulta longa do SQS por ciclo (`ReceiveMessageCommand.WaitTimeSeconds`). */
  waitTimeSeconds?: number; // padrão 10
  maxMessages?: number; // padrão 10 (teto SQS)
  /** Igual ao `VisibilityTimeout` da fila (§5: 30s). */
  visibilityTimeoutSeconds?: number; // padrão 30
  /** Renovação periódica da visibilidade em lote. */
  heartbeatMs?: number; // padrão 20000
  /** Espera entre ciclos vazios (a consulta longa já é a espera principal). */
  pollIntervalMs?: number; // padrão 100
  /** Nome fixo do consumidor para deduplicação da inbox. */
  consumerName?: string; // padrão 'wager-sqs-consumer'
  retryBaseDelaySeconds?: number; // padrão 30
  retryMaxDelaySeconds?: number; // padrão 3600
  /**
   * Gancho de teste/injeção de falha: roda antes do DELETE (ack); lançar dele
   * simula morte pós-commit: a mensagem NÃO é deletada e a reentrega
   * é deduplicada pela inbox.
   */
  beforeAck?: (messageId: string) => Promise<void>;
  /** Registro da casa (fatia 8b): tentativas SQS e duplicata por inbox. */
  metrics?: MetricsPort;
}

interface ConsumerRuntimeOptions
  extends Required<Omit<SqsWagerConsumerOptions, 'enabled' | 'beforeAck' | 'metrics'>> {}

function withDefaults(options: SqsWagerConsumerOptions): ConsumerRuntimeOptions {
  return {
    waitTimeSeconds: Math.min(options.waitTimeSeconds ?? 10, 20),
    maxMessages: Math.min(options.maxMessages ?? 10, 10),
    visibilityTimeoutSeconds: options.visibilityTimeoutSeconds ?? 30,
    heartbeatMs: options.heartbeatMs ?? 20_000,
    pollIntervalMs: options.pollIntervalMs ?? 100,
    consumerName: options.consumerName ?? SQS_WAGER_CONSUMER_NAME,
    retryBaseDelaySeconds: options.retryBaseDelaySeconds ?? 30,
    retryMaxDelaySeconds: options.retryMaxDelaySeconds ?? 3_600,
  };
}

/** Registro injetado à parte (não faz parte das opções numéricas de execução). */
function withMetrics(options: SqsWagerConsumerOptions): MetricsPort | undefined {
  return options.metrics;
}

/**
 * Consumidor da `wager-transactions.fifo` (ARCHITECTURE §5 / CHALLENGE §10).
 * Por mensagem, na MESMA tx do use case: inbox (dedup persistente) + submit;
 * DELETE (ack) só pós-commit nos casos terminais. Transitório e payload
 * inválido NÃO deletam — visibilidade devolve a mensagem (redrive → DLQ).
 */
@Injectable()
export class SqsWagerConsumer implements OnModuleInit, OnApplicationShutdown {
  private readonly options: ConsumerRuntimeOptions;
  private readonly enabled: boolean;
  private readonly beforeAck?: (messageId: string) => Promise<void>;
  private running = false;
  private loopPromise?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private resolveSleep?: () => void;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private queueUrlPromise?: Promise<string>;
  /** Recibo → timeout atual (para alinhar a renovação ao backoff). */
  private inFlight = new Map<string, number>();

  private readonly useCaseMetrics?: MetricsPort;

  constructor(
    private readonly useCase: SubmitWagerCommands,
    private readonly client: SQSClient,
    private readonly queueName: string,
    options: SqsWagerConsumerOptions,
  ) {
    this.options = withDefaults(options);
    this.enabled = options.enabled;
    this.beforeAck = options.beforeAck;
    this.useCaseMetrics = options.metrics;
  }

  onModuleInit(): void {
    if (this.enabled) {
      this.start();
    }
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.heartbeatTimer = setInterval(() => void this.heartbeat(), this.options.heartbeatMs);
    this.loopPromise = this.loop();
  }

  /** Desligamento limpo (SIGTERM): para consulta e renovação, conclui o que está em voo. */
  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
    if (this.timer) {
      clearTimeout(this.timer);
    }
    const wake = this.resolveSleep;
    this.resolveSleep = undefined;
    wake?.();
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  /**
   * Um ciclo: recebe até `maxMessages` pela consulta longa e processa em sequência
   * (ordem FIFO por grupo). Retorno = quantidade recebida (0 = fila vazia).
   */
  async cycle(): Promise<number> {
    const response = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await this.resolveQueueUrl(),
        MaxNumberOfMessages: this.options.maxMessages,
        WaitTimeSeconds: this.options.waitTimeSeconds,
        VisibilityTimeout: this.options.visibilityTimeoutSeconds,
        // `ApproximateReceiveCount` não está no enum do SDK, mas a SQS aceita o campo.
        AttributeNames: ['ApproximateReceiveCount' as QueueAttributeName],
      }),
    );
    const messages = response.Messages ?? [];
    for (const message of messages) {
      this.inFlight.set(message.ReceiptHandle!, this.options.visibilityTimeoutSeconds);
    }
    // Processa em sequência para preservar a ordem FIFO de cada grupo.
    for (const message of messages) {
      try {
        await this.process(message as SqsMessage);
      } finally {
        this.inFlight.delete(message.ReceiptHandle!);
      }
    }
    return messages.length;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      let received: number;
      try {
        received = await this.cycle();
      } catch (error) {
        received = 0;
        this.log('warn', 'sqs.cycle_failed', { error: String(error) });
      }
      if (received === 0) {
        await this.sleep(this.options.pollIntervalMs);
      }
    }
  }

  private async process(message: SqsMessage): Promise<void> {
    const parsedReceiveCount = Number.parseInt(message.Attributes?.ApproximateReceiveCount ?? '1', 10);
    const receiveCount = Number.isFinite(parsedReceiveCount) && parsedReceiveCount > 0
      ? parsedReceiveCount
      : 1;
    const body = message.Body ?? '';

    const envelope = this.parseEnvelope(body);
    if (envelope === undefined) {
      // Envelope ilegível ou tipo desconhecido: erro permanente — NÃO deleta; o redrive
      // da fila leva à DLQ após `maxReceiveCount` (5).
      this.log('warn', 'sqs.envelope_invalid', { sqsMessageId: message.MessageId });
      return;
    }

    // Escopo de log da mensagem SQS: correlationId = messageId (escolha única
    // para traçar a mensagem do recebimento à publicação do evento original).
    await withLogContextFields({ messageId: envelope.messageId, correlationId: envelope.messageId }, async () =>
      this.processEnvelope(message, envelope, body, receiveCount),
    );
  }

  private async processEnvelope(
    message: SqsMessage,
    envelope: { messageId: string; data: unknown },
    body: string,
    receiveCount: number,
  ): Promise<void> {
    // Conta a nova tentativa a partir da 2ª entrega (visibilidade vencida ou adiada).
    if (receiveCount >= 2) {
      this.useCaseMetrics?.increment(SQS_RETRIES_TOTAL);
    }

    const input = this.buildInput(envelope);
    if (input === undefined) {
      // Payload inválido (inclui kind OPENING, money, idempotencyKey ausente):
      // permanente — NÃO deleta e não abre tx; redrive → DLQ.
      this.log('warn', 'sqs.payload_invalid', {
        sqsMessageId: message.MessageId,
        messageId: envelope.messageId,
      });
      return;
    }

    let result: SubmitWagerResult;
    // A reentrega só conta se o commit acontecer: o contador entra
    // APÓS o `execute` (mesma semântica do ack).
    let inboxPayloadConflict = false;
    try {
      result = await this.useCase.execute(input, {
        beforeRepositories: async (repositories) => {
          // Commit confirma o consumo: a linha só sobrevive junto com o efeito.
          const inboxMessage = InboxMessage.receive({
            consumerName: this.options.consumerName,
            messageId: envelope.messageId,
            payloadHash: createHash('sha256').update(body).digest('hex'),
            receivedAt: new Date(),
          });
          inboxMessage.markProcessed(new Date());
          const inserted = await repositories.inbox.tryInsert(inboxMessage);
          if (!inserted.inserted) {
            inboxPayloadConflict = inserted.existingPayloadHash !== inboxMessage.payloadHash;
            return 'DUPLICATE';
          }
          return 'CONTINUE';
        },
      });
    } catch (error) {
      // Transitório (PG fora, lock timeout…): NÃO deleta; aumenta a visibilidade
      // exponencialmente conforme `ApproximateReceiveCount`.
      this.log('error', 'sqs.process_deferred', {
        messageId: envelope.messageId,
        walletId: input.walletId,
        receiveCount,
        retryDelaySeconds: this.retryDelaySeconds(receiveCount),
        error: String(error),
      });
      await this.deferWithBackoff(message, receiveCount);
      return;
    }

    if (result.outcome === 'INBOX_DUPLICATE') {
      this.useCaseMetrics?.increment(WAGER_DUPLICATES_TOTAL, { source: 'inbox' });
      this.log(
        inboxPayloadConflict ? 'warn' : 'info',
        inboxPayloadConflict ? 'sqs.message_conflict' : 'sqs.inbox_duplicate',
        {
          messageId: envelope.messageId,
          receiveCount,
          payloadHashConflict: inboxPayloadConflict,
        },
      );
    } else {
      this.log('info', 'sqs.processed', {
        messageId: envelope.messageId,
        transactionId: 'transaction' in result ? result.transaction.id : undefined,
        outcome: result.outcome,
        reply: 'replay' in result ? result.replay : undefined,
      });
    }

    // Gancho de teste: encerra o processo após o commit e antes do primeiro DELETE.
    // A inbox torna a reentrega sem efeito. Restrito a NODE_ENV=test.
    if (process.env.NODE_ENV === 'test' && process.env.FAULT_EXIT_AFTER_COMMIT === '1') {
      this.log('warn', 'test.fault_exit', {
        messageId: envelope.messageId,
        stage: 'after_commit_before_ack',
      });
      process.exit(137);
    }

    // Commit concluído: confirma o caso terminal. beforeAck simula falha de teste;
    // se lançar, a mensagem volta a ser recebida para reprocessamento.
    await this.beforeAck?.(envelope.messageId);
    try {
      await this.client.send(
        new DeleteMessageCommand({
          QueueUrl: await this.resolveQueueUrl(),
          ReceiptHandle: message.ReceiptHandle!,
        }),
      );
    } catch (error) {
      this.log('warn', 'sqs.ack_deferred', {
        messageId: envelope.messageId,
        receiveCount,
        retryDelaySeconds: this.retryDelaySeconds(receiveCount),
        error: String(error),
      });
      await this.deferWithBackoff(message, receiveCount);
      return;
    }
    this.log('info', 'sqs.acked', { messageId: envelope.messageId });
  }

  /** Envelope CHALLENGE §10: `{ messageId, type, occurredAt, data }`. */
  private parseEnvelope(body: string): { messageId: string; data: unknown } | undefined {
    let raw: unknown;
    try {
      raw = JSON.parse(body);
    } catch {
      return undefined;
    }
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      return undefined;
    }
    const envelope = raw as Record<string, unknown>;
    if (typeof envelope.messageId !== 'string' || envelope.messageId.length === 0) {
      return undefined;
    }
    if (envelope.type !== 'WagerTransactionRequested') {
      return undefined;
    }
    return { messageId: envelope.messageId, data: envelope.data };
  }

  /**
   * Monta o input pelo MESMO validador da borda HTTP (`application/wager-input`
   * — inclui "zero só em LOSS"); `Idempotency-Key` vem de `data.idempotencyKey`
   * e o `messageId` vira `correlationId` (propaga para a outbox).
   */
  private buildInput(envelope: {
    messageId: string;
    data: unknown;
  }): SubmitWagerInput | undefined {
    try {
      const body: WagerInputBody = parseWagerInput(envelope.data);
      const data = envelope.data as Record<string, unknown>;
      const idempotencyKey = data.idempotencyKey;
      if (typeof idempotencyKey !== 'string' || idempotencyKey.trim().length === 0) {
        throw new WagerInputError('idempotencyKey must be a non-empty string');
      }
      return {
        idempotencyKey,
        providerId: body.providerId,
        externalTransactionId: body.externalTransactionId,
        playerId: body.playerId,
        walletId: body.walletId,
        roundId: body.roundId,
        gameId: body.gameId,
        kind: body.kind,
        money: body.money,
        referenceExternalTransactionId: body.referenceExternalTransactionId,
        correlationId: envelope.messageId,
      };
    } catch (error) {
      if (error instanceof WagerInputError) {
        return undefined;
      }
      throw error;
    }
  }

  private retryDelaySeconds(receiveCount: number): number {
    return sqsRetryDelaySeconds(
      receiveCount,
      this.options.retryBaseDelaySeconds,
      this.options.retryMaxDelaySeconds,
    );
  }

  /** Falha transitória: oculta a mensagem por 30s, 60s, 120s... até 1h. */
  private async deferWithBackoff(message: SqsMessage, receiveCount: number): Promise<void> {
    const receiptHandle = message.ReceiptHandle;
    if (receiptHandle === undefined) {
      return;
    }
    const visibilityTimeout = this.retryDelaySeconds(receiveCount);
    this.inFlight.set(receiptHandle, visibilityTimeout);
    try {
      await this.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: await this.resolveQueueUrl(),
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: visibilityTimeout,
        }),
      );
    } catch (error) {
      this.log('warn', 'sqs.retry_visibility_failed', {
        messageId: message.MessageId,
        receiveCount,
        retryDelaySeconds: visibilityTimeout,
        error: String(error),
      });
    }
  }

  /** Renova a visibilidade das mensagens em voo (lotes de até 10). */
  private async heartbeat(): Promise<void> {
    if (!this.running || this.inFlight.size === 0) {
      return;
    }
    const entries = [...this.inFlight.entries()].slice(0, 10).map(([receiptHandle, timeout], index) => ({
      Id: String(index),
      ReceiptHandle: receiptHandle,
      VisibilityTimeout: timeout,
    }));
    if (entries.length === 0) {
      return;
    }
    try {
      await this.client.send(
        new ChangeMessageVisibilityBatchCommand({
          QueueUrl: await this.resolveQueueUrl(),
          Entries: entries,
        }),
      );
    } catch (error) {
      this.log('warn', 'sqs.heartbeat_failed', { error: String(error) });
    }
  }

  private async resolveQueueUrl(): Promise<string> {
    this.queueUrlPromise ??= this.client
      .send(new GetQueueUrlCommand({ QueueName: this.queueName }))
      .then((response) => {
        if (!response.QueueUrl) {
          throw new Error(`SQS queue "${this.queueName}" não tem QueueUrl`);
        }
        return response.QueueUrl;
      })
      .catch((error) => {
        // Falha transitória ao resolver a fila não fica em cache.
        this.queueUrlPromise = undefined;
        throw error;
      });
    return this.queueUrlPromise;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      this.resolveSleep = resolve;
      this.timer = setTimeout(() => {
        this.resolveSleep = undefined;
        resolve();
      }, ms);
    });
  }

  private log(
    level: 'info' | 'warn' | 'error',
    event: string,
    fields: Record<string, unknown>,
  ): void {
    logEvent(level, event, fields);
  }
}
