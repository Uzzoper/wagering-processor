import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';
import { loadConfig } from './infra/config/env';

async function bootstrap(): Promise<void> {
  const { port } = loadConfig();
  const app = await NestFactory.create(AppModule);

  await app.listen(port);

  console.log(JSON.stringify({ level: 'info', event: 'http.listening', port }));
}

void bootstrap();
