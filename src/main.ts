import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';
import { loadConfig } from './infra/config/env';
import { JsonLogger, configuredLogLevel, logEvent } from './infra/observability/json-logger';
import { correlationMiddleware } from './infra/observability/correlation.middleware';

async function bootstrap(): Promise<void> {
  const { port, workers, logLevel } = loadConfig();
  configuredLogLevel(logLevel);
  const appLogger = new JsonLogger();
  const app = await NestFactory.create(AppModule, { logger: appLogger });
  app.useLogger(appLogger);
  // Boot, requests e workers saem todos por aqui (uma linha JSON por evento).
  // Borda HTTP: gera/eco X-Correlation-Id e abre o escopo ALS do request.
  app.use(correlationMiddleware);

  // Importante para OnApplicationShutdown (consumer SQS para o poll e worker da
  // outbox para o polling: encerram o loop gravidamente).
  app.enableShutdownHooks();

  if (workers.enabled.includes('api')) {
    await app.listen(port);
    logEvent('info', 'http.listening', { port });
  } else {
    // Sem listener HTTP o event loop do Bun encerra o processo logo depois dos
    // hooks: keepalive é obrigatório para worker-only (sqs/outbox/pending).
    setInterval(() => {}, 1 << 30);
  }

  // Encerramento determinístico do processo: os workers já chegaram a um estado
  // seguro no banco (mensagens do SQS/replay do inbox, lease da outbox) — a
  // invariante "nada perdido" vem da transação, não do shutdown gracioso.
  process.on('SIGTERM', () => process.exit(0));
}

void bootstrap();
