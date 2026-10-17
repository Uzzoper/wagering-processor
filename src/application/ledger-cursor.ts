import { WagerInputError, isUuid } from './wager-input';

/**
 * Posição opaca do cursor do ledger: par `(created_at, id)` que fecha a
 * página. O timestamp é a string ISO com microssegundos vinda do POSGRES
 * (`Date` só cobre ms — truncar derrubaria linhas do mesmo ms no keyset).
 */
export interface LedgerCursor {
  createdAt: string;
  id: string;
}

/** base64url (alfabeto seguro em URL) — sem `=` de padding na saída. */
const CURSOR_ALPHABET = /^[A-Za-z0-9_-]+$/;
const CURSOR_KEYS_EXACT = new Set(['createdAt', 'id']);
/** ISO-8601 canônico UTC com microssegundos opcionais (até 6 dígitos). */
const CURSOR_ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/;

/**
 * Codifica o cursor: base64url de JSON `{ createdAt, id }`. Opaco para a
 * borda: o conteúdo só importa para `decodeLedgerCursor`.
 */
export function encodeLedgerCursor(cursor: LedgerCursor): string {
  const json = JSON.stringify({ createdAt: cursor.createdAt, id: cursor.id });
  return Buffer.from(json, 'utf8').toString('base64url');
}

/**
 * Decodifica o cursor com rejeição total: alfabeto inválido, JSON inválido,
 * campos ausentes/extras, timestamp não-canônico ou id não-UUID →
 * `WagerInputError` (`400 INVALID_PAYLOAD` na borda).
 */
export function decodeLedgerCursor(value: string): LedgerCursor {
  if (!CURSOR_ALPHABET.test(value)) {
    throw new WagerInputError('ledger cursor is malformed');
  }

  let raw: string;
  try {
    raw = Buffer.from(value, 'base64url').toString('utf8');
  } catch {
    throw new WagerInputError('ledger cursor is malformed');
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new WagerInputError('ledger cursor payload is not valid JSON');
  }

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new WagerInputError('ledger cursor payload must be an object');
  }
  const fields = new Set(Object.keys(parsed as Record<string, unknown>));
  if (fields.size !== CURSOR_KEYS_EXACT.size || ![...fields].every((key) => CURSOR_KEYS_EXACT.has(key))) {
    throw new WagerInputError('ledger cursor payload must have exactly { createdAt, id }');
  }

  const { createdAt, id } = parsed as Record<string, unknown>;
  if (typeof createdAt !== 'string' || CURSOR_ISO.test(createdAt) === false || Number.isNaN(Date.parse(createdAt))) {
    throw new WagerInputError('ledger cursor createdAt must be a canonical UTC ISO timestamp');
  }
  if (typeof id !== 'string' || !isUuid(id)) {
    throw new WagerInputError('ledger cursor id must be a valid UUID');
  }

  const cursor: LedgerCursor = { createdAt, id };
  // Round-trip: base64 truncado/recheado volta a JSON válido? então descarta.
  if (encodeLedgerCursor(cursor) !== value) {
    throw new WagerInputError('ledger cursor is malformed');
  }
  return cursor;
}
