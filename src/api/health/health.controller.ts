import { MikroORM } from '@mikro-orm/postgresql';
import {
  Controller,
  Get,
  Inject,
  Res,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

/** Timeout por check (CHALLENGE §12: readiness não pendura o endpoint). */
const CHECK_TIMEOUT_MS = 2_000;

/** Mínimo do Express usado aqui; evita depender dos tipos do framework. */
interface StatusResponse {
  status(code: number): unknown;
}

export interface ReadinessResponse {
  status: 'ok' | 'unavailable';
  checks: { postgres: 'up' | 'down'; sqs: 'up' | 'down' };
}

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout de ${ms}ms no health check`)), ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

export interface HealthDependencies {
  sqs: SQSClient;
  wagerQueueName: string;
}

/**
 * Readiness (CHALLENGE §12): Postgres (`SELECT 1`) e SQS (`GetQueueUrl` da
 * fila de transações configurada), 2s de prazo por check. Sem dado sensível
 * (nenhum ARN/URL na resposta). `/health/live` segue aberto e sem dependências.
 */
@Controller('health')
export class HealthController {
  private queueUrlPromise?: Promise<string>;

  constructor(
    private readonly orm: MikroORM,
    @Inject('HealthSqsDeps') private readonly deps: HealthDependencies,
  ) {
    this.sqs = deps.sqs;
    this.queueName = deps.wagerQueueName;
  }

  private readonly sqs: SQSClient;
  private readonly queueName: string;

  @Get('live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(
    @Res({ passthrough: true }) res: StatusResponse,
  ): Promise<ReadinessResponse> {
    const checks = {
      postgres: 'down' as 'up' | 'down',
      sqs: 'down' as 'up' | 'down',
    };
    try {
      await withTimeout(
        this.orm.em.getConnection().execute('SELECT 1'),
        CHECK_TIMEOUT_MS,
      );
      checks.postgres = 'up';
    } catch {
      // check mapeado como down (sem throw: readiness responde 503 estruturado)
    }
    try {
      await withTimeout(this.resolveQueueUrl(this.queueName), CHECK_TIMEOUT_MS);
      checks.sqs = 'up';
    } catch {
      // check mapeado como down
    }

    const ok = checks.postgres === 'up' && checks.sqs === 'up';
    res.status(ok ? 200 : 503);
    return { status: ok ? 'ok' : 'unavailable', checks };
  }

  private resolveQueueUrl(queueName: string): Promise<string> {
    this.queueUrlPromise ??= this.sqs
      .send(new GetQueueUrlCommand({ QueueName: queueName }))
      .then((response) => {
        if (!response.QueueUrl) {
          throw new Error(`SQS queue "${queueName}" não tem QueueUrl`);
        }
        return response.QueueUrl;
      })
      .catch((error) => {
        // Falha transitória da resolução não é memorizada.
        this.queueUrlPromise = undefined;
        throw error;
      });
    return this.queueUrlPromise;
  }
}
