import { defineEntity } from '@mikro-orm/core';

/**
 * Espelho fino da tabela `outbox_message`. Só colunas + índices.
 * `next_attempt_at` nunca é NULL e tem default `now()` no banco.
 */
export const OutboxMessageEntity = defineEntity({
  name: 'OutboxMessage',
  tableName: 'outbox_message',
  properties: (p) => ({
    id: p.uuid().primary(),
    eventId: p.uuid().fieldName('event_id'),
    aggregateId: p.uuid().fieldName('aggregate_id'),
    eventType: p.string().length(255).fieldName('event_type'),
    payload: p.json<Record<string, unknown>>(),
    occurredAt: p.datetime().fieldName('occurred_at'),
    attempts: p.integer().default(0),
    nextAttemptAt: p.datetime().fieldName('next_attempt_at').defaultRaw('now()'),
    publishedAt: p.datetime().fieldName('published_at').nullable(),
  }),
  uniques: [{ name: 'uq_outbox_event', properties: ['eventId'] }],
  indexes: [
    { name: 'ix_outbox_pending', properties: ['nextAttemptAt'], where: 'published_at IS NULL' },
  ],
});
