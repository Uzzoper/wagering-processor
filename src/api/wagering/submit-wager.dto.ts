import { FailureCodes } from '../../domain/failure-code';
import {
  WagerInputError,
  isUuid,
  parseWagerInput,
  type WagerInputBody,
} from '../../application/wager-input';
import { BusinessError } from '../errors/business-error';

export type SubmitWagerBody = WagerInputBody;
export { isUuid };
export type { WagerInputBody };

/**
 * Validador/normalizador do corpo de `POST /wagering/transactions` — thin
 * adapter: a regra mora em `application/wager-input` (mesmo parser do consumer
 * SQS, incluindo "zero só em LOSS"); aqui só converte a falha de entrada para
 * o erro da borda HTTP (`400 INVALID_PAYLOAD`).
 */
export function parseSubmitWagerBody(input: unknown): SubmitWagerBody {
  try {
    return parseWagerInput(input);
  } catch (error) {
    if (error instanceof WagerInputError) {
      throw new BusinessError(FailureCodes.InvalidPayload, error.message);
    }
    throw error;
  }
}
