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

function statusFor(code: FailureCode): number {
  switch (code) {
    case FailureCodes.InvalidPayload:
      return 400;
    case FailureCodes.WalletConflict:
      return 409;
    case FailureCodes.WalletNotFound:
      return 404;
    case FailureCodes.InfraTransient:
      return 503;
    default:
      return 500;
  }
}

function failureCodeForStatus(status: number): FailureCode {
  switch (status) {
    case 400:
      return FailureCodes.InvalidPayload;
    case 404:
      return FailureCodes.WalletNotFound;
    case 409:
      return FailureCodes.WalletConflict;
    default:
      return FailureCodes.InfraTransient;
  }
}
