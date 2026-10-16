import { describe, expect, test } from 'bun:test';

import { WalletBalanceChanged, type IntegrationEventJson } from '../../src/domain/events';
import { Money } from '../../src/domain/money';
import { OutboxMessage } from '../../src/domain/outbox-message';
import { Wallet } from '../../src/domain/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/wallet-ledger-entry';

const AT = new Date('2026-03-01T12:00:00.000Z');

function brl(amount: string): Money {
  return Money.from({ amount, currency: 'BRL' });
}

/** Evento real do domínio para exercitar `enqueue` sem infraestrutura. */
function balanceChangedEvent(): WalletBalanceChanged {
  const wallet = Wallet.rehydrate({
    id: 'wallet-1',
    playerId: 'player-1',
    currency: 'BRL',
    balance: brl('75.00'),
    version: 3,
    createdAt: AT,
    updatedAt: AT,
  });
  const entry = WalletLedgerEntry.create({
    id: 'ledger-1',
    walletId: 'wallet-1',
    transactionId: 'tx-1',
    direction: LedgerDirection.Debit,
    money: brl('25.00'),
    balanceBefore: brl('100.00'),
    balanceAfter: brl('75.00'),
    createdAt: AT,
  });

  return WalletBalanceChanged.from(wallet, entry, { correlationId: 'corr-1' });
}

describe('OutboxMessage.enqueue', () => {
  test('nasce do envelope do evento: id/eventId, payload JSON e primeira tentativa agora', () => {
    const event = balanceChangedEvent();
    const message = OutboxMessage.enqueue(event);

    expect(message.id).toBe(event.eventId);
    expect(message.eventId).toBe(event.eventId);
    expect(message.aggregateId).toBe(event.aggregateId);
    expect(message.eventType).toBe('WalletBalanceChanged');
    expect(message.occurredAt).toBe(event.occurredAt);
    expect(message.attempts).toBe(0);
    expect(message.nextAttemptAt).toBe(event.occurredAt);
  });

  test('payload é o envelope serializado completo', () => {
    const event = balanceChangedEvent();
    const message = OutboxMessage.enqueue(event);

    const envelope = event.toJSON();
    expect(message.payload).toEqual(envelope as unknown as Readonly<Record<string, unknown>>);
    expect((message.payload as unknown as IntegrationEventJson<unknown>).eventType).toBe(
      'WalletBalanceChanged',
    );
    expect(message.payload.data).toEqual(envelope.data);
  });
});

describe('OutboxMessage — fila e agendamento', () => {
  test('nova mensagem está pendente e vencida', () => {
    const event = balanceChangedEvent();
    const message = OutboxMessage.enqueue(event);

    expect(message.isPending()).toBe(true);
    expect(message.isDue(event.occurredAt)).toBe(true);
    expect(message.isDue(new Date(event.occurredAt.getTime() - 1))).toBe(false);
  });

  test('não está vencida antes de next_attempt_at (retry futuro)', () => {
    const message = OutboxMessage.enqueue(balanceChangedEvent());
    const futureDue = new Date(AT.getTime() + 4_000);

    message.scheduleRetry(AT); // próxima tentativa em +2s
    expect(message.isDue(new Date(AT.getTime() + 1_999))).toBe(false);
    expect(message.isDue(new Date(AT.getTime() + 2_000))).toBe(true);
    expect(message.isDue(futureDue)).toBe(true);
  });

  test('publicada sai da fila e para de estar vencida', () => {
    const message = OutboxMessage.enqueue(balanceChangedEvent());
    const publishedAt = new Date(AT.getTime() + 1_000);

    message.markPublished(publishedAt);

    expect(message.publishedAt).toBe(publishedAt);
    expect(message.isPending()).toBe(false);
    expect(message.isDue(new Date(AT.getTime() + 10_000))).toBe(false);
  });

  test('rehydrate reconstrói estado persistido sem revalidar', () => {
    const occurredAt = new Date('2026-03-01T10:00:00.000Z');
    const nextAttemptAt = new Date('2026-03-01T10:05:00.000Z');
    const publishedAt = new Date('2026-03-01T10:03:00.000Z');

    const message = OutboxMessage.rehydrate({
      id: 'outbox-9',
      eventId: 'event-9',
      aggregateId: 'wallet-9',
      eventType: 'WagerTransactionProcessed',
      payload: { eventId: 'event-9', version: 1 },
      occurredAt,
      attempts: 4,
      nextAttemptAt,
      publishedAt,
    });

    expect(message.id).toBe('outbox-9');
    expect(message.eventId).toBe('event-9');
    expect(message.attempts).toBe(4);
    expect(message.nextAttemptAt).toBe(nextAttemptAt);
    expect(message.publishedAt).toBe(publishedAt);
    expect(message.isPending()).toBe(false);
  });
});

describe('OutboxMessage.scheduleRetry — backoff exponencial com teto', () => {
  test('progressão 2s → 4s → … → 4min16s (8ª) → teto 5min (9ª)', () => {
    const message = OutboxMessage.enqueue(balanceChangedEvent());
    const expectedOffsetsMs = [2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000];

    for (const [index, offset] of expectedOffsetsMs.entries()) {
      message.scheduleRetry(AT);
      const next = message.nextAttemptAt!;
      expect(next.getTime()).toBe(AT.getTime() + offset);
      expect(message.attempts).toBe(index + 1);
    }

    // Teto: a 10ª tentativa continua nos 5min.
    message.scheduleRetry(AT);
    expect(message.nextAttemptAt!.getTime()).toBe(AT.getTime() + 300_000);
  });

  test('retry parte do `now` informado, não do occurredAt', () => {
    const message = OutboxMessage.enqueue(balanceChangedEvent());
    const later = new Date(AT.getTime() + 60_000);

    message.scheduleRetry(later);
    expect(message.nextAttemptAt!.getTime()).toBe(later.getTime() + 2_000);
  });
});
