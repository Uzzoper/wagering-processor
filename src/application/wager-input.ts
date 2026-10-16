import { WagerTransactionKind, type WagerTransactionKind as Kind } from '../domain/wager-transaction';

const AMOUNT = /^\d+\.\d{2}$/;
const CURRENCY = /^[A-Z]{3}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuid(value: string): boolean {
  return UUID.test(value);
}

/**
 * Erro de entrada inválida (borda, sem `failureCode` de negócio — é `400`).
 * Carrega o failureCode estável: HTTP mapeia para status; SQS classifica como
 * payload permanentemente inválido (sem ack — redrive leva à DLQ).
 */
export class WagerInputError extends Error {
  readonly failureCode = 'INVALID_PAYLOAD' as const;

  constructor(message: string) {
    super(message);
    this.name = 'WagerInputError';
  }
}

/** Corpo validado de submissão (HTTP e SQS compartilham exatamente este parser). */
export interface WagerInputBody {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: { amount: string; currency: string };
  /** Ausente = `null`; `REFUND`/`ROLLBACK` exigem não-vazio. */
  referenceExternalTransactionId: string | null;
}

function parseKind(value: unknown): WagerTransactionKind {
  switch (value) {
    case 'BET':
      return WagerTransactionKind.Bet;
    case 'WIN':
      return WagerTransactionKind.Win;
    case 'LOSS':
      return WagerTransactionKind.Loss;
    case 'REFUND':
      return WagerTransactionKind.Refund;
    case 'ROLLBACK':
      return WagerTransactionKind.Rollback;
    default:
      throw invalidPayload('kind must be one of BET, WIN, LOSS, REFUND, ROLLBACK');
  }
}

/**
 * Valida/normaliza o corpo de submissão de transação. Única fonte da regra de
 * borda (inclui "zero só em LOSS"); `OPENING` nunca passa aqui. Falha vira
 * `INVALID_PAYLOAD` sem persistir nada.
 */
export function parseWagerInput(input: unknown): WagerInputBody {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    throw invalidPayload('body must be a JSON object');
  }

  const body = input as Record<string, unknown>;

  const providerId = requiredString(body.providerId, 'providerId');
  const externalTransactionId = requiredString(body.externalTransactionId, 'externalTransactionId');
  const roundId = requiredString(body.roundId, 'roundId');
  const gameId = requiredString(body.gameId, 'gameId');

  const playerId = body.playerId;
  if (typeof playerId !== 'string' || !isUuid(playerId)) {
    throw invalidPayload('playerId must be a valid UUID');
  }
  const walletId = body.walletId;
  if (typeof walletId !== 'string' || !isUuid(walletId)) {
    throw invalidPayload('walletId must be a valid UUID');
  }

  const kind = parseKind(body.kind);

  const money = body.money;
  if (typeof money !== 'object' || money === null || Array.isArray(money)) {
    throw invalidPayload('money must be an object with amount and currency');
  }
  const { amount, currency } = money as Record<string, unknown>;
  if (typeof amount !== 'string' || !AMOUNT.test(amount)) {
    throw invalidPayload('money.amount must match ^\\d+\\.\\d{2}$');
  }
  if (typeof currency !== 'string' || !CURRENCY.test(currency)) {
    throw invalidPayload('money.currency must be ISO-4217 (3 uppercase letters)');
  }
  // Zero só em LOSS (ck_wager_money_pos); demais exigem valor positivo.
  if (kind !== WagerTransactionKind.Loss && amount === '0.00') {
    throw invalidPayload('money.amount must be positive for BET, WIN, REFUND and ROLLBACK');
  }

  const reference = body.referenceExternalTransactionId;
  if (kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback) {
    if (typeof reference !== 'string' || reference.length === 0) {
      throw invalidPayload('referenceExternalTransactionId is required for REFUND and ROLLBACK');
    }
    return {
      providerId,
      externalTransactionId,
      playerId,
      walletId,
      roundId,
      gameId,
      kind,
      money: { amount, currency },
      referenceExternalTransactionId: reference,
    };
  }
  if (reference !== undefined && reference !== null) {
    if (typeof reference !== 'string' || reference.length === 0) {
      throw invalidPayload('referenceExternalTransactionId must be a non-empty string when present');
    }
    return {
      providerId,
      externalTransactionId,
      playerId,
      walletId,
      roundId,
      gameId,
      kind,
      money: { amount, currency },
      referenceExternalTransactionId: reference,
    };
  }
  return {
    providerId,
    externalTransactionId,
    playerId,
    walletId,
    roundId,
    gameId,
    kind,
    money: { amount, currency },
    referenceExternalTransactionId: null,
  };
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw invalidPayload(`${name} must be a non-empty string`);
  }
  return value;
}

function invalidPayload(message: string): WagerInputError {
  return new WagerInputError(message);
}
