import { randomUUID } from 'node:crypto';

import type { LogContext } from './log-context';
import { runWithLogContext, sanitizeCorrelationId } from './log-context';

/**
 * Somos compatíveis com o Express mínimo usado no projeto (sem tipagem do framework).

/**
 * Somos compatíveis com o Express mínimo usado no projeto (sem tipagem do framework).
 */
interface CorrelationRequest {
  headers?: Record<string, unknown>;
}

interface CorrelationResponse {
  setHeader(name: string, value: string): unknown;
}

type NextFunction = () => void;

/**
 * Borda HTTP (CHALLENGE §12): lê/gera `X-Correlation-Id`, ecoa no header da
 * resposta e abre o escopo ALS do request — correlationId chega ao use case e
 * aos logs sem passar por cada camada.
 */
export function correlationMiddleware(
  request: CorrelationRequest,
  response: CorrelationResponse,
  next: NextFunction,
): void {
  const header = request.headers?.['x-correlation-id'];
  const incoming = Array.isArray(header) ? header[0] : header;
  const found = sanitizeCorrelationId(incoming);
  const context: LogContext = {
    correlationId: found ?? crypto.randomUUID(),
  };
  response.setHeader('X-Correlation-Id', context.correlationId!);
  runWithLogContext(context, next);
}
