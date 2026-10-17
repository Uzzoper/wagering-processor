import type { EntityManager } from '@mikro-orm/postgresql';

import type { InboxInsertResult, InboxStore } from '../../application/ports';
import type { InboxMessage } from '../../domain/inbox-message';

/**
 * Dedup persistente do consumidor SQS. Nunca captura `23505` (abortaria a tx):
 * `ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING` identifica
 * redelivery e lê o hash original. A linha só sobrevive junto com o commit da
 * tx — por isso `processed_at` já entra preenchido.
 */
export class MikroOrmInboxStore implements InboxStore {
  constructor(private readonly em: EntityManager) {}

  async tryInsert(message: InboxMessage): Promise<InboxInsertResult> {
    const rows = await this.em.execute<Array<{ id: string }>>(
      `INSERT INTO inbox_message (id, consumer_name, message_id, payload_hash, received_at, processed_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (consumer_name, message_id) DO NOTHING
       RETURNING id`,
      [
        crypto.randomUUID(),
        message.consumerName,
        message.messageId,
        message.payloadHash,
        message.receivedAt,
        message.processedAt ?? null,
      ],
    );
    if (rows.length > 0) {
      return { inserted: true };
    }

    const existing = await this.em.execute<Array<{ payload_hash: string }>>(
      `SELECT payload_hash
       FROM inbox_message
       WHERE consumer_name = ? AND message_id = ?`,
      [message.consumerName, message.messageId],
    );
    if (existing[0] === undefined) {
      throw new Error('Inbox conflict row was not visible after ON CONFLICT');
    }
    return { inserted: false, existingPayloadHash: existing[0].payload_hash };
  }
}
