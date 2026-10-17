import { randomUUID } from 'node:crypto';

import { runWithLogContext, sanitizeCorrelationId } from './log-context';

/**
 * Somos compatíveis com o Express mínimo usado no projeto (sem tipagem do framework).

/**
 * Somos compatíveis com o Express mínimo usado no projeto (sem tipagem do framework).
 */
interface CorrelationRequest {
  headers?: Record<string, unknown>;
  /** A borda entrega o valor ao use case (controller) sem importar o ALS. */
  correlationId?: string;
}

interface CorrelationResponse {
  setHeader(name: string, value: string): unknown;
}

type NextFunction = () => void;

/**
 * Borda HTTP (CHALLENGE §12): lê/gera `X-Correlation-Id`, ecoa no header da
 * resposta e abre o escopo ALS do request — correlationId chega ao use case e
 * aos logs sem passar por cada camada. Também grava o valor no request para o
 * controller consumir (a `api` não importa `infra`).
 */
export function correlationMiddleware(
  request: CorrelationRequest,
  response: CorrelationResponse,
  next: NextFunction,
): void {
  const header = request.headers?.['x-correlation-id'];
  const incoming = Array.isArray(header) ? header[0] : header;
  const found = sanitizeCorrelationId(incoming);
  request.correlationId = found ?? randomUUID();
  response.setHeader('X-Correlation-Id', request.correlationId);
  runWithLogContext({ correlationId: request.correlationId }, next);
}
