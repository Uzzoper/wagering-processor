import { ArgumentsHost, Catch, HttpException, type ExceptionFilter } from '@nestjs/common';

import { FailureCodes, type FailureCode } from '../../domain/failure-code';
import { BusinessError } from '../errors/business-error';

/** Só o que usamos de `Response`; evita depender dos tipos do Express. */
interface HttpResponse {
  status(code: number): { json(body: unknown): void };
}

/** Corpo de erro travado: `{ failureCode, message }` — código estável para o provedor. */
export interface ErrorResponseBody {
  failureCode: string;
  message: string;
}

/**
 * Filtro único de exceção. Rejeição de negócio chega como `BusinessError`
 * (mapa do §7); o resto é infraestrutura → `503 INFRA_TRANSIENT` (nunca vira
 * `REJECTED` de negócio).
 */
@Catch()
export class FailureExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<HttpResponse>();
    const body = mapFailure(exception);
    response.status(body.status).json({ failureCode: body.failureCode, message: body.message });
  }
}

function mapFailure(exception: unknown): ErrorResponseBody & { status: number } {
  if (exception instanceof BusinessError) {
    return {
      status: statusFor(exception.failureCode),
      failureCode: exception.failureCode,
      message: exception.message,
    };
  }

  if (exception instanceof HttpException) {
    const status = exception.getStatus();
    return { status, failureCode: failureCodeForStatus(status), message: exception.message };
  }

  return {
    status: 503,
    failureCode: FailureCodes.InfraTransient,
    message: exception instanceof Error ? exception.message : 'unexpected error',
  };
}

/** Fonte única da verdade: `FailureCode → HTTP`. O inverso deriva daqui. */
const FAILURE_HTTP_STATUS = new Map<FailureCode, number>([
  [FailureCodes.InvalidPayload, 400],
  [FailureCodes.WalletNotFound, 404],
  [FailureCodes.TransactionNotFound, 404],
  [FailureCodes.WalletConflict, 409],
  [FailureCodes.IdempotencyConflict, 409],
  [FailureCodes.InsufficientBalance, 422],
  [FailureCodes.ReversalWouldOverdraw, 422],
  [FailureCodes.InvalidReference, 422],
  [FailureCodes.AlreadyReversed, 422],
  [FailureCodes.AmountMismatch, 422],
  [FailureCodes.CurrencyMismatch, 422],
  [FailureCodes.WalletOwnerMismatch, 422],
  [FailureCodes.InvalidProvider, 422],
  [FailureCodes.InfraTransient, 503],
]);

function statusFor(code: FailureCode): number {
  return FAILURE_HTTP_STATUS.get(code) ?? 500;
}

function failureCodeForStatus(status: number): FailureCode {
  for (const [code, mapped] of FAILURE_HTTP_STATUS) {
    if (mapped === status) return code;
  }
  return FailureCodes.InfraTransient;
}
