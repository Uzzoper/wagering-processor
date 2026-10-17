import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';
import { loadConfig } from './infra/config/env';

async function bootstrap(): Promise<void> {
  const { port, workers } = loadConfig();
  const app = await NestFactory.create(AppModule);

  // Importante para OnApplicationShutdown (consumer SQS para o poll e worker da
  // outbox para o polling: encerram o loop gravidamente).
  app.enableShutdownHooks();

  if (workers.enabled.includes('api')) {
    await app.listen(port);
    console.log(JSON.stringify({ level: 'info', event: 'http.listening', port }));
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
