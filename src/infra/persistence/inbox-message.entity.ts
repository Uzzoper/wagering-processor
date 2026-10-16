import { defineEntity } from '@mikro-orm/core';

/**
 * Espelho fino da tabela `inbox_message`. Só colunas + índices.
 * Deduplicação do consumidor SQS por `(consumer_name, message_id)`.
 */
export const InboxMessageEntity = defineEntity({
  name: 'InboxMessage',
  tableName: 'inbox_message',
  properties: (p) => ({
    id: p.uuid().primary(),
    consumerName: p.string().length(255).fieldName('consumer_name'),
    messageId: p.string().length(255).fieldName('message_id'),
    payloadHash: p.string().length(64).fieldName('payload_hash'),
    receivedAt: p.datetime().fieldName('received_at'),
    processedAt: p.datetime().fieldName('processed_at').nullable(),
  }),
  uniques: [{ name: 'uq_inbox_consumer_msg', properties: ['consumerName', 'messageId'] }],
});
