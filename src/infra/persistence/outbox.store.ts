import type { EntityManager } from '@mikro-orm/postgresql';

import type { OutboxStore } from '../../application/ports';
import type { IntegrationEvent } from '../../domain/events';
import { OutboxMessage } from '../../domain/outbox-message';

/**
 * Grava o envelope na outbox dentro da mesma tx. `next_attempt_at` usa o default
 * `now()` do banco, então o claim do worker já enxerga a linha vencida.
 */
export class MikroOrmOutboxStore implements OutboxStore {
  constructor(private readonly em: EntityManager) {}

  async insert(events: IntegrationEvent<unknown>[]): Promise<void> {
    for (const event of events) {
      await this.em.execute(
        `INSERT INTO outbox_message (id, event_id, aggregate_id, event_type, payload, occurred_at)
         VALUES (?, ?, ?, ?, ?::jsonb, ?)`,
        [
          crypto.randomUUID(),
          event.eventId,
          event.aggregateId,
          event.eventType,
          JSON.stringify(event.toJSON()),
          event.occurredAt,
        ],
      );
    }
  }

  /**
   * Claim com lease (ARCHITECTURE §5): tx curta que roda `UPDATE ... FOR UPDATE
   * SKIP LOCKED` — dois publishers nunca claimam a mesma linha; a linha claimada
   * volta a ficar vencida quando o lease expira (cobre morte pós-claim).
   */
  async claimDue(limit: number, leaseMs: number): Promise<OutboxMessage[]> {
    const rows = await this.em.execute<Record<string, unknown>[]>(
      `UPDATE outbox_message
          SET next_attempt_at = now() + make_interval(secs => ?)
        WHERE id IN (
          SELECT id FROM outbox_message
           WHERE published_at IS NULL AND next_attempt_at <= now()
           ORDER BY occurred_at
           LIMIT ?
           FOR UPDATE SKIP LOCKED
        )
        RETURNING id, event_id, aggregate_id, event_type, payload, occurred_at,
                  attempts, next_attempt_at, published_at`,
      [leaseMs / 1000, limit],
    );
    return rows.map((row) => toDomain(row));
  }

  async markPublished(id: string): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_message SET published_at = now() WHERE id = ? AND published_at IS NULL`,
      [id],
    );
  }

  async persistRetry(id: string, attempts: number, nextAttemptAt: Date): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_message SET attempts = ?, next_attempt_at = ? WHERE id = ? AND published_at IS NULL`,
      [attempts, nextAttemptAt, id],
    );
  }
}

/** Reconstrução via domínio (`rehydrate` — sem revalidação). */
function toDomain(row: Record<string, unknown>): OutboxMessage {
  return OutboxMessage.rehydrate({
    id: row.id as string,
    eventId: row.event_id as string,
    aggregateId: row.aggregate_id as string,
    eventType: row.event_type as string,
    payload: row.payload as Record<string, unknown>,
    occurredAt: toDate(row.occurred_at) ?? failMissing('occurred_at'),
    attempts: row.attempts as number,
    nextAttemptAt: toDate(row.next_attempt_at),
    publishedAt: toDate(row.published_at),
  });
}

/** `em.execute` cru devolve timestamptz como string (pg sem parser de ts). */
function toDate(value: unknown): Date | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (value instanceof Date) {
    return value;
  }
  return new Date(String(value).replace(' ', 'T').replace('+00', 'Z').replace('+0000', 'Z'));
}

function failMissing(column: string): never {
  throw new Error(`outbox_message: coluna "${column}" ausente no claim`);
}
