import type { LoggerService } from '@nestjs/common';

import { currentLogContext } from './log-context';

/** Níveis válidos de `LOG_LEVEL` (default `info`); `silent` desliga tudo. */
export type LogLevelName = 'debug' | 'info' | 'error' | 'silent';
/** `warn/error` só chegam via evento, não via `LOG_LEVEL` do usuário. */
export type EmittingLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_RANK: Record<EmittingLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

/** Limiar corrente do processo (eventos da casa + logs do framework). */
let threshold: EmittingLevel = 'info';
let silent = false;

export function configuredLogLevel(level: LogLevelName): void {
  if (level === 'silent') {
    silent = true;
  } else {
    silent = false;
    threshold = level;
  }
}

function enabled(level: EmittingLevel): boolean {
  return !silent && LEVEL_RANK[level] >= LEVEL_RANK[threshold];
}

function emit(level: EmittingLevel, record: Record<string, unknown>): void {
  const line = JSON.stringify({ level, ...record, timestamp: new Date().toISOString() });
  // Stderr para severidade alta; stdout para o resto (padrão do 12-factor).
  if (level === 'error') {
    console.error(line);
  } else if (level === 'warn') {
    console.warn(line);
  } else {
    console.log(line);
  }
}

/**
 * Evento da casa: `{ level, event, ...campos, timestamp }` — contexto via ALS
 * entra automático (correlationId etc.) a menos que o caller passe explícito.
 */
export function logEvent(
  level: EmittingLevel,
  event: string,
  fields: Record<string, unknown> = {},
): void {
  if (!enabled(level)) {
    return;
  }
  const context = currentLogContext();
  const known: Record<string, string> = {};
  for (const [key, value] of Object.entries(context)) {
    if (value !== undefined) {
      known[key] = value;
    }
  }
  emit(level, { event, ...known, ...fields });
}

/**
 * Logger JSON (uma linha) do Nest — cobre logs de boot do framework e o resto.
 * `message` do core é texto: vira `msg`; objetos nossos podem passar `msg`.
 */
export class JsonLogger implements LoggerService {
  log(message: unknown, context?: string): void {
    this.write('info', message, context);
  }

  error(message: unknown, stackOrContext?: string, context?: string): void {
    this.write('error', message, context, stackOrContext);
  }

  warn(message: unknown, context?: string): void {
    this.write('warn', message, context);
  }

  debug(message: unknown, context?: string): void {
    this.write('debug', message, context);
  }

  verbose(message: unknown, context?: string): void {
    this.write('debug', message, context);
  }

  fatal(message: unknown, context?: string): void {
    this.write('error', message, context);
  }

  private write(level: EmittingLevel, message: unknown, context?: string, stack?: string): void {
    if (!enabled(level)) {
      return;
    }
    const text = typeof message === 'string' ? message : JSON.stringify(message);
    emit(level, {
      event: 'nest.log',
      ...(context !== undefined ? { context } : {}),
      msg: text,
      ...(stack !== undefined && level === 'error' ? { stack } : {}),
    });
  }
}
