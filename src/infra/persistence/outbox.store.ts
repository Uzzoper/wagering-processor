import type { EntityManager } from '@mikro-orm/postgresql';

import type { OutboxStore } from '../../application/ports';
import type { IntegrationEvent } from '../../domain/events';

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
}
