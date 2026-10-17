import { resolve } from 'node:path';

/**
 * Fatia 7d — processo REAL do app (`bun run src/main.ts`) para os testes de
 * multiprocessamento e crash recovery. O filho herda o `process.env` do
 * runner (preload já aponta `DB_NAME=wagering_test` e credenciais do compose).
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
}

export interface SpawnAppOptions {
  port: number;
  /** CSV de `WORKERS_ENABLED` (p.ex. `api,outbox,pending`). */
  workers: string;
  /** Extras exclusive do cenário (SQS_WAGER_QUEUE, FAULT_*, leases curtos…). */
  env?: Record<string, string | undefined>;
}

/** Sobe o app e espera `GET /health/live` (poll com deadline, padrão 20s). */
export async function spawnApp(options: SpawnAppOptions): Promise<SpawnedApp> {
  const { port } = options;
  const proc = Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(port),
      WORKERS_ENABLED: options.workers,
      NODE_ENV: 'test',
      ...options.env,
    },
    stdout: 'ignore',
    stderr: 'inherit',
  });

  const deadline = Date.now() + 20_000;
  for (;;) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health/live`);
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
      return Number(await proc.exited);
    },
  };
}

export type SpawnApp = Awaited<ReturnType<typeof spawnApp>>;
