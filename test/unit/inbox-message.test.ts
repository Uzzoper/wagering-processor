import { describe, expect, test } from 'bun:test';

import { InboxMessage } from '../../src/domain/inbox-message';

describe('InboxMessage.receive', () => {
  test('nasce pendente com receivedAt default', () => {
    const message = InboxMessage.receive({
      consumerName: 'wager-sqs-consumer',
      messageId: 'msg-123',
      payloadHash: 'a'.repeat(64),
    });

    expect(message.consumerName).toBe('wager-sqs-consumer');
    expect(message.messageId).toBe('msg-123');
    expect(message.payloadHash).toBe('a'.repeat(64));
    expect(message.receivedAt).toBeInstanceOf(Date);
    expect(message.isProcessed()).toBe(false);
    expect(message.processedAt).toBeUndefined();
  });

  test('receivedAt explícito é respeitado', () => {
    const receivedAt = new Date('2026-01-01T00:00:00.000Z');
    const message = InboxMessage.receive({
      consumerName: 'c',
      messageId: 'm',
      payloadHash: 'h',
      receivedAt,
    });

    expect(message.receivedAt).toBe(receivedAt);
  });
});

describe('InboxMessage.markProcessed', () => {
  test('marca como processada', () => {
    const message = InboxMessage.receive({
      consumerName: 'wager-sqs-consumer',
      messageId: 'msg-123',
      payloadHash: 'h',
    });

    const at = new Date('2026-02-02T10:00:00.000Z');
    message.markProcessed(at);

    expect(message.isProcessed()).toBe(true);
    expect(message.processedAt).toBe(at);
  });
});

describe('InboxMessage.rehydrate', () => {
  test('reconstrói estado persistido sem revalidar', () => {
    const state = {
      consumerName: 'wager-sqs-consumer',
      messageId: 'msg-456',
      payloadHash: 'b'.repeat(64),
      receivedAt: new Date('2026-03-03T00:00:00.000Z'),
      processedAt: new Date('2026-03-03T01:00:00.000Z'),
    };

    const message = InboxMessage.rehydrate(state);

    expect(message).toMatchObject({
      consumerName: state.consumerName,
      messageId: state.messageId,
      payloadHash: state.payloadHash,
      receivedAt: state.receivedAt,
    });
    expect(message.isProcessed()).toBe(true);
    expect(message.processedAt).toEqual(state.processedAt);
  });

  test('estado sem processedAt permanece pendente', () => {
    const message = InboxMessage.rehydrate({
      consumerName: 'c',
      messageId: 'm',
      payloadHash: 'h',
      receivedAt: new Date(),
    });

    expect(message.isProcessed()).toBe(false);
  });
});
