import { AsyncLocalStorage } from 'node:async_hooks';

/**
 * Contexto de log por operação (CHALLENGE §12): só identificadores — nunca
 * valores monetários nem payloads. Vive em `AsyncLocalStorage`, então request
 * HTTP e ciclo/tarefa de worker levam seus campos a qualquer log intermediário.
 */
export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const store = new AsyncLocalStorage<LogContext>();

const KNOWN_FIELDS = [
  'correlationId',
  'messageId',
  'transactionId',
  'walletId',
  'providerId',
] as const;

/** Campos da operação corrente ({} fora de qualquer escopo). */
export function currentLogContext(): LogContext {
  const stored = store.getStore();
  const fields: Record<string, string> = {};
  if (stored !== undefined) {
    for (const key of KNOWN_FIELDS) {
      const value = stored[key];
      if (value !== undefined) {
        fields[key] = value;
      }
    }
  }
  return fields;
}

/** Escopo novo (borda: request HTTP, mensagem SQS, ciclo de worker). */
export function runWithLogContext<T>(context: LogContext, fn: () => T): T {
  return store.run(clean(context), fn);
}

/** União do contexto corrente com campos novos (um aluno microsserviço: por mensagem/ciclo). */
export function withLogContextFields<T>(fields: LogContext, fn: () => T): T {
  const current = store.getStore();
  if (current === undefined) {
    return store.run(clean(fields), fn);
  }
  return store.run({ ...clean(current), ...clean(fields) }, fn);
}

/**
 * `X-Correlation-Id` da borda: trim, teto de 200 caracteres e sem quebra de
 * linha (proteção contra injeção de log). Vazio/ausente → caller gera UUID.
 */
export function sanitizeCorrelationId(raw: unknown): string | undefined {
  if (typeof raw !== 'string') {
    return undefined;
  }
  const value = raw.trim().replaceAll('\r', ' ').replaceAll('\n', ' ');
  if (value.length === 0 || value.length > 200) {
    return undefined;
  }
  return value;
}

/** Descarta campos vazios e não contemplados antes de entrar no escopo. */
function clean(context: LogContext): LogContext {
  const fields: Record<string, string> = {};
  for (const key of KNOWN_FIELDS) {
    const value = context[key];
    if (typeof value === 'string' && value.length > 0) {
      fields[key] = value;
    }
  }
  return fields;
}
