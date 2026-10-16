import type { IntegrationEvent, IntegrationEventJson } from './events';

/** Estado reconstruído por `rehydrate` e persistido na tabela `outbox_message`. */
export interface OutboxMessageState {
  id: string;
  eventId: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

/** Teto do backoff exponencial. Progressão completa documentada em `scheduleRetry`. */
const RETRY_BACKOFF_CAP_MS = 5 * 60_000;

/**
 * Atraso do backoff exponencial para a tentativa `attempt` (1-based), teto de 5min:
 * `2s → 4s → 8s → … → 256s (8ª) → 300s fixo a partir da 9ª`.
 */
function backoffMs(attempts: number): number {
  return Math.min(RETRY_BACKOFF_CAP_MS, 2_000 * 2 ** (attempts - 1));
}

/**
 * Mensagem da outbox (CHALLENGE §6.5). Nasce de `enqueue` do envelope do evento,
 * virando a linha `outbox_message`; o worker publica depois do commit.
 * Imutável estruturalmente: só `scheduleRetry`/`markPublished` movem tentativa e datas.
 */
export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly eventId: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  /** Cria a mensagem a partir do evento; `payload` é o envelope serializado. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    // Envelope do evento é o payload estável da outbox (§11).
    const payload: Record<string, unknown> = { ...event.toJSON() };
    return new OutboxMessage(
      event.eventId,
      event.eventId,
      event.aggregateId,
      event.eventType,
      payload,
      event.occurredAt,
      0,
      event.occurredAt,
    );
  }

  /** Reconstrução a partir da persistência — não revalida. */
  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.eventId,
      state.aggregateId,
      state.eventType,
      state.payload,
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  /** Pendente enquanto não publicada — o worker varre exatamente o `published_at IS NULL`. */
  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  /** Vencida para claim: pendente e com `next_attempt_at <= now` (nunca NULL por construção). */
  isDue(now: Date): boolean {
    return this.isPending() && this._nextAttemptAt !== undefined && this._nextAttemptAt <= now;
  }

  /** Publicada → sai da fila; `published_at` nunca vem de volta. */
  markPublished(at: Date): void {
    this._publishedAt = at;
  }

  /**
   * Incrementa tentativas e agenda o retry com backoff exponencial com teto:
   * tentativa 1 → +2s, 2 → +4s, 3 → +8s, 4 → +16s, 5 → +32s, 6 → +1min04s,
   * 7 → +2min08s, 8 → +4min16s, 9 em diante → teto de 5min.
   */
  scheduleRetry(now: Date): void {
    this._attempts += 1;
    this._nextAttemptAt = new Date(now.getTime() + backoffMs(this._attempts));
  }
}
