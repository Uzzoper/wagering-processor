import { createHash } from 'node:crypto';

/** Campos de negócio do hash (ARCHITECTURE §4); `amount` já vem normalizado (2 casas) da borda. */
export interface PayloadHashInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  amount: string;
  currency: string;
  referenceExternalTransactionId?: string | null;
}

/** Ordena chaves recursivamente; header e metadados de transporte nunca entram aqui. */
function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const entries = Object.keys(value as Record<string, unknown>)
      .sort()
      .map((key) => [key, sortKeys((value as Record<string, unknown>)[key])]);
    return Object.fromEntries(entries);
  }
  return value;
}

/**
 * SHA-256 hex do JSON canônico (chaves ordenadas, UTF-8, sem espaços).
 * Referência ausente = `null`, então `undefined` e `null` geram o mesmo hash.
 */
export function payloadHash(input: PayloadHashInput): string {
  const normalized = {
    providerId: input.providerId,
    externalTransactionId: input.externalTransactionId,
    playerId: input.playerId,
    walletId: input.walletId,
    roundId: input.roundId,
    gameId: input.gameId,
    kind: input.kind,
    amount: input.amount,
    currency: input.currency,
    referenceExternalTransactionId: input.referenceExternalTransactionId ?? null,
  };
  return createHash('sha256').update(JSON.stringify(sortKeys(normalized))).digest('hex');
}
