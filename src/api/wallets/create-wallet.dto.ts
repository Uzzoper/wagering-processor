import { FailureCodes } from '../../domain/failure-code';
import type { MoneyProps } from '../../domain/money';
import { BusinessError } from '../errors/business-error';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AMOUNT = /^\d+\.\d{2}$/;

export interface CreateWalletBody {
  playerId: string;
  initialBalance: MoneyProps;
}

export function isUuid(value: string): boolean {
  return UUID.test(value);
}

/**
 * Valida/normaliza o corpo de `POST /wallets`. A regex de `amount` já rejeita
 * `25.1`, `1e3` e negativo; falha de borda vira `INVALID_PAYLOAD` (`400`).
 */
export function parseCreateWalletBody(input: unknown): CreateWalletBody {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    throw invalidPayload('body must be a JSON object');
  }

  const body = input as Record<string, unknown>;

  const playerId = body.playerId;
  if (typeof playerId !== 'string' || !UUID.test(playerId)) {
    throw invalidPayload('playerId must be a valid UUID');
  }

  const balance = body.initialBalance;
  if (typeof balance !== 'object' || balance === null || Array.isArray(balance)) {
    throw invalidPayload('initialBalance must be an object with amount and currency');
  }

  const { amount, currency } = balance as Record<string, unknown>;
  if (typeof amount !== 'string' || !AMOUNT.test(amount)) {
    throw invalidPayload('initialBalance.amount must match ^\\d+\\.\\d{2}$');
  }
  // ISO-4217: exatamente 3 letras maiúsculas (o domínio revalida via `Money.from`).
  if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) {
    throw invalidPayload('initialBalance.currency must be ISO-4217 (3 uppercase letters)');
  }

  return { playerId, initialBalance: { amount, currency } };
}

function invalidPayload(message: string): BusinessError {
  return new BusinessError(FailureCodes.InvalidPayload, message);
}
