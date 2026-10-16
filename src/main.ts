import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';
import { loadConfig } from './infra/config/env';

async function bootstrap(): Promise<void> {
  const { port, workers } = loadConfig();
  const app = await NestFactory.create(AppModule);

  // Importante para OnApplicationShutdown (worker da outbox: parar o polling e
  // terminar o lote em andamento).
  app.enableShutdownHooks();

  if (workers.enabled.includes('api')) {
    await app.listen(port);
    console.log(JSON.stringify({ level: 'info', event: 'http.listening', port }));
  }
}

void bootstrap();
