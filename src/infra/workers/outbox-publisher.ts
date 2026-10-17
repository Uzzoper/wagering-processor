import {
  Inject,
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';

import {
  EVENT_PUBLISHER,
  UNIT_OF_WORK,
  type EventPublisherPort,
  type MetricsPort,
  type UnitOfWork,
} from '../../application/ports';
import { OUTBOX_PUBLISH_ERRORS_TOTAL } from '../../application/metric-names';
import type { OutboxMessage } from '../../domain/outbox-message';
import { logEvent } from '../observability/json-logger';

export interface OutboxPublisherOptions {
  /** `WORKERS_ENABLED` contém `outbox`. */
  enabled: boolean;
  pollIntervalMs: number;
  batchSize: number;
  leaseMs: number;
  /** Registry da casa (fatia 8b): erros de publicação. */
  metrics?: MetricsPort;
}

/**
 * Worker da outbox (ARCHITECTURE §5): claim com lease em tx curta, publica na
 * fila FORA da transação financeira e só então marca `published_at` (tx curta).
 * Falha → `OutboxMessage.scheduleRetry` persistido (backoff 2s → … → teto 5min).
 * Morte entre claim e publicação é coberta pelo lease: outro publisher assume.
 */
@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnApplicationShutdown {
  private running = false;
  private loopPromise?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private resolveSleep?: () => void;

  constructor(
    @Inject(UNIT_OF_WORK) private readonly unitOfWork: UnitOfWork,
    @Inject(EVENT_PUBLISHER) private readonly publisher: EventPublisherPort,
    private readonly options: OutboxPublisherOptions,
  ) {}

  private incrementPublishError(): void {
    this.options.metrics?.increment(OUTBOX_PUBLISH_ERRORS_TOTAL);
  }

  onModuleInit(): void {
    if (this.options.enabled) {
      this.start();
    }
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.loopPromise = this.loop();
  }

  /** Shutdown limpo: para o polling e termina o lote em andamento. */
  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    if (this.timer !== undefined) {
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
   * Um ciclo: claim → publicação → persistência do estado de cada mensagem.
   * Retorno = quantidade claimada (0 = outbox vazia neste momento).
   */
  async processDueBatch(): Promise<number> {
    // Tx curta de claim (lock_timeout de 3s aplicado pela UnitOfWork).
    const claim = await this.unitOfWork.run((repositories) =>
      repositories.outbox.claimDue(this.options.batchSize, this.options.leaseMs),
    );
    if (claim.length === 0) {
      return 0;
    }

    // Gancho de crash recovery (teste, fatia 7d): mata o processo REAL logo
    // após o claim (lease tomado) e antes da primeira publicação — a linha
    // volta a ficar vencida ao expirar o lease. Somente em NODE_ENV=test e,
    // se FAULT_EXIT_CLAIM_AGGREGATE estiver definido, só quando o lote contém
    // uma linha desse aggregate (o boot pode claimar linhas alheias da suíte).
    const faultAggregate = process.env.FAULT_EXIT_CLAIM_AGGREGATE;
    const shouldFault =
      process.env.NODE_ENV === 'test' &&
      process.env.FAULT_EXIT_AFTER_CLAIM === '1' &&
      (faultAggregate === undefined ||
        claim.some((message) => message.aggregateId === faultAggregate));
    if (shouldFault) {
      logEvent('warn', 'test.fault_exit', {
        stage: 'after_claim_before_publish',
        claimed: claim.length,
      });
      process.exit(137);
    }

    let published = 0;
    for (const message of claim) {
      // Sequencial para preservar a ordem de occurredAt por MessageGroupId.
      try {
        await this.publish(message);
        await this.unitOfWork.run((repositories) =>
          repositories.outbox.markPublished(message.id),
        );
        published += 1;
      } catch (error) {
        await this.retryLater(message, error);
      }
    }
    return published;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      let claimed: number;
      try {
        claimed = await this.processDueBatch();
      } catch (error) {
        claimed = 0;
        logEvent('warn', 'outbox.cycle_failed', { error: String(error) });
      }
      if (claimed === 0) {
        await this.sleep(this.options.pollIntervalMs);
      }
    }
  }

  private publish(message: OutboxMessage): Promise<void> {
    return this.publisher.publish({
      eventId: message.eventId,
      aggregateId: message.aggregateId,
      payload: JSON.stringify(message.payload),
    });
  }

  private async retryLater(message: OutboxMessage, error: unknown): Promise<void> {
    this.incrementPublishError();
    message.scheduleRetry(new Date());
    const nextAttemptAt = message.nextAttemptAt;
    if (nextAttemptAt === undefined) {
      return;
    }
    await this.unitOfWork.run((repositories) =>
      repositories.outbox.persistRetry(message.id, message.attempts, nextAttemptAt),
    );
    logEvent('warn', 'outbox.publish_failed', {
      eventId: message.eventId,
      walletId: message.aggregateId,
      attempts: message.attempts,
      error: String(error),
    });
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
}
