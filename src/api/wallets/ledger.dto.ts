import { decodeLedgerCursor, type LedgerCursor } from '../../application/ledger-cursor';
import { LEDGER_LIMIT_DEFAULT, LEDGER_LIMIT_MAX } from '../../application/ports';
import { FailureCodes } from '../../domain/failure-code';
import type { MoneyProps } from '../../domain/money';
import { WagerInputError } from '../../application/wager-input';
import { BusinessError } from '../errors/business-error';

/** Corpo de `GET /wallets/:walletId/ledger` (padrão JSON da casa: `MoneyProps`). */
export interface LedgerItem {
  id: string;
  transactionId: string;
  direction: 'DEBIT' | 'CREDIT';
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerResponse {
  items: LedgerItem[];
  nextCursor: string | null;
}

/** Corpo de `POST /wallets/:walletId/reconciliation` (shape exato do §9). */
export interface ReconciliationResponse {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: string;
  consistent: boolean;
  checkedEntries: number;
}

const LIMIT_DIGITS = /^\d+$/;

export interface LedgerQuery {
  cursor: LedgerCursor | undefined;
  limit: number;
}

/**
 * Valida `?limit=&cursor=`. `limit` precisa ser inteiro positivo até 200
 * (default 50); `cursor` é base64url opaco decodificado pelo codec puro.
 * Falha de borda → `INVALID_PAYLOAD` (`400`), reusando a tradução da casa.
 */
export function parseLedgerQuery(query: Record<string, unknown>): LedgerQuery {
  return {
    limit: parseLimit(query.limit),
    cursor: parseCursor(query.cursor),
  };
}

function parseLimit(raw: unknown): number {
  if (raw === undefined || raw === '') {
    return LEDGER_LIMIT_DEFAULT;
  }
  if (typeof raw !== 'string' || LIMIT_DIGITS.test(raw) === false) {
    throw invalidPayload('limit must be a positive integer');
  }
  const limit = Number(raw);
  // 0, negativo (não passa no regex) e acima do teto são payload inválido.
  if (limit < 1 || limit > LEDGER_LIMIT_MAX) {
    throw invalidPayload(`limit must be between 1 and ${LEDGER_LIMIT_MAX}`);
  }
  return limit;
}

function parseCursor(raw: unknown): LedgerCursor | undefined {
  if (raw === undefined || raw === '') {
    return undefined;
  }
  if (typeof raw !== 'string') {
    throw invalidPayload('cursor must be an opaque base64url string');
  }
  try {
    return decodeLedgerCursor(raw);
  } catch (error) {
    if (error instanceof WagerInputError) {
      throw invalidPayload(error.message);
    }
    throw error;
  }
}

function invalidPayload(message: string): BusinessError {
  return new BusinessError(FailureCodes.InvalidPayload, message);
}
