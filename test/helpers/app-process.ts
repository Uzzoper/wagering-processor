import { resolve } from 'node:path';

/**
 * Fatia 7d — processo REAL do app (`bun run src/main.ts`) para os testes de
 * multiprocessamento e crash recovery. O filho herda o `process.env` do
 * runner, incluindo banco de teste e defaults SQS/LocalStack do preload.
 */

const ROOT = resolve(import.meta.dir, '..', '..');

export interface SpawnedApp {
  port: number;
  baseUrl: string;
  proc: Bun.Subprocess;
  /** Sinal cru (SIGKILL para crash, SIGTERM para shutdown limpo). */
  kill(signal: 'SIGTERM' | 'SIGKILL'): void;
  /** Shutdown limpo: SIGTERM e espera o `exited` do processo. */
  stop(): Promise<number>;
  /** stdout completo (só com `silent: false`): linhas JSON do logger. */
  stdoutText(): Promise<string>;
}

export interface SpawnAppOptions {
  port: number;
  /** CSV de `WORKERS_ENABLED` (p.ex. `api,outbox,pending`). */
  workers: string;
  /** Extras exclusive do cenário (SQS_WAGER_QUEUE, FAULT_*, leases curtos…). */
  env?: Record<string, string | undefined>;
  /**
   * `false` captura o stdout do filho (suíte de logs da fatia 8b); default
   * `silent` descarta (outras suítes rodam com `LOG_LEVEL=silent` via preload).
   */
  silent?: boolean;
}

/** Filhos vivos da suíte: no exit do runner (mesmo com teste falhado ou
 * beforeAll abortado), SIGKILL de cauda sincrônico — nenhum app sobrevive. */
const ALIVE = new Set<Bun.Subprocess>();

process.on('exit', () => {
  for (const child of ALIVE) {
    try {
      child.kill('SIGKILL');
    } catch {
      // já saído
    }
  }
});

/** A porta precisa estar LIVRE antes do spawn: um processo órfão de rodada
 * interrompida responde /health/live e a readiness "valida" o processo morto
 * (o filho real morre de EADDRINUSE). Pre-checagem converte isso em falha
 * rápida e clara em vez de teste correndo contra um serviço estranho. */
async function assertPortFree(port: number): Promise<void> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/health/live`);
    if (response.ok || response.status >= 400) {
      throw new Error(`porta ${port} já ocupada por processo órfão (limpe antes de rodar)`);
    }
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('porta ')) {
      throw error;
    }
    // fetch recusado = porta livre
  }
}

/** Sobe o app e espera `GET /health/live` (poll com deadline, padrão 20s). */
export async function spawnApp(options: SpawnAppOptions): Promise<SpawnedApp> {
  const { port } = options;
  await assertPortFree(port);
  const proc = Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(port),
      WORKERS_ENABLED: options.workers,
      NODE_ENV: 'test',
      ...options.env,
    },
    stdout: options.silent === false ? 'pipe' : 'ignore',
    stderr: 'inherit',
  });
  ALIVE.add(proc);
  void proc.exited.then(() => ALIVE.delete(proc));

  // Suíte de logs: acumula as linhas JSON do stdout enquanto o app vive.
  const stdoutChunks: Uint8Array[] = [];
  if (options.silent === false && proc.stdout) {
    void collectStream(proc.stdout, stdoutChunks);
  }

  const deadline = Date.now() + 20_000;
  for (;;) {
    try {
    const response = await fetch(`http://127.0.0.1:${port}/health/live`, {
      signal: AbortSignal.timeout(1_500),
    });
      if (response.ok) {
        break;
      }
    } catch {
      // ainda não subiu: novo poll
    }
    if (proc.exitCode !== null) {
      throw new Error(`app da porta ${port} morreu antes de ficar pronto`);
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: app da porta ${port} não respondeu /health/live`);
    }
    await Bun.sleep(200);
  }
  // Margem pós-readiness: saúde pode ter vindo de processo órfão na porta.
  // Se o filho de verdade morrer nesse intervalo (EADDRINUSE...), falha aqui.
  await Bun.sleep(1_000);
  if (proc.exitCode !== null || proc.signalCode !== null) {
    throw new Error(`app da porta ${port} morreu logo após a readiness (porta roubada?)`);
  }

  return {
    port,
    baseUrl: `http://127.0.0.1:${port}`,
    proc,
    kill(signal) {
      try {
        proc.kill(signal);
      } catch {
        // já saído: nada a fazer
      }
    },
    async stop() {
      try {
        proc.kill('SIGTERM');
      } catch {
        // já saído
      }
      // SIGTERM com prazo; sem resposta limpa, SIGKILL — o afterAll nunca
      // pendura nem deixa órfão, com teste verde ou falhado.
      const deadline = Date.now() + 5_000;
      while (proc.exitCode === null && proc.signalCode === null && Date.now() < deadline) {
        await Bun.sleep(100);
      }
      if (proc.exitCode === null && proc.signalCode === null) {
        try {
          proc.kill('SIGKILL');
        } catch {
          // já saído
        }
      }
      return Number(await Promise.race([proc.exited, Bun.sleep(3_000).then(() => -1)]));
    },
    async stdoutText() {
      return new TextDecoder().decode(concatChunks(stdoutChunks));
    },
  };
}

async function collectStream(stream: ReadableStream<Uint8Array>, chunks: Uint8Array[]): Promise<void> {
  try {
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value !== undefined) {
        chunks.push(value);
      }
    }
  } catch {
    // stream fechado no kill: o que chegou basta para os asserts dos logs
  }
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

/** Para todos os apps da suíte de uma vez (nunca lança; usado no afterAll). */
export async function stopAllApps(apps: readonly SpawnApp[]): Promise<void> {
  await Promise.all(apps.map((app) => app.stop().catch(() => undefined)));
}

export type SpawnApp = Awaited<ReturnType<typeof spawnApp>>;
