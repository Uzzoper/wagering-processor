import type { EntityManager } from '@mikro-orm/postgresql';

import type { InboxStore } from '../../application/ports';
import type { InboxMessage } from '../../domain/inbox-message';

/**
 * Dedup persistente do consumidor SQS. Nunca captura `23505` (abortaria a tx):
 * `ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING` devolve
 * `false` para redelivery. A linha só sobrevive junto com o commit da tx —
 * commit implica consumo efetivo, por isso `processed_at` já entra preenchido.
 */
export class MikroOrmInboxStore implements InboxStore {
  constructor(private readonly em: EntityManager) {}

  async tryInsert(message: InboxMessage): Promise<boolean> {
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
    return rows.length > 0;
  }
}
