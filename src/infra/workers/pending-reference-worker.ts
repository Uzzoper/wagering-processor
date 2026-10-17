import type { OnApplicationShutdown, OnModuleInit } from '@nestjs/common';

import {
  type SubmitWagerCommands,
  type SubmitWagerInput,
} from '../../application/submit-wager.use-case';
import type { Clock, StoredTransaction, UnitOfWork } from '../../application/ports';
import { SYSTEM_CLOCK } from '../config/clock';
import { logEvent } from '../observability/json-logger';

export interface PendingReferenceWorkerOptions {
  /** `WORKERS_ENABLED` contém `pending`. */
  enabled: boolean;
  pollIntervalMs: number;
  batchSize?: number; // default 50
  /** Lease do claim sobre `next_retry_at` (linha volta vencida após o lease). */
  leaseMs?: number; // default 60_000
  /** TTL de 24h desde o primeiro defer (ARCHITECTURE §5). */
  ttlHours?: number; // default 24
  maxAttempts?: number; // default 20
  /** Clock injetável (testes usam fake); default `SYSTEM_CLOCK`. */
  clock?: Clock;
}

interface PendingRefRuntimeOptions
  extends Required<Omit<PendingReferenceWorkerOptions, 'enabled' | 'clock'>> {}

function withDefaults(options: PendingReferenceWorkerOptions): PendingRefRuntimeOptions {
  return {
    pollIntervalMs: options.pollIntervalMs,
    batchSize: options.batchSize ?? 50,
    leaseMs: options.leaseMs ?? 60_000,
    ttlHours: options.ttlHours ?? 24,
    maxAttempts: options.maxAttempts ?? 20,
  };
}

/**
 * Worker de `PENDING_REFERENCE` (ARCHITECTURE §5 / CHALLENGE §7.1):
 * reprocessa REFUND/ROLLBACK fora de ordem pelo MESMO use case de submissão
 * (`pendingResolution` → resume em vez do replay 202). Claim com lease +
 * `SKIP LOCKED` (arquétipo do worker da outbox).
 */
export class PendingReferenceWorker implements OnModuleInit, OnApplicationShutdown {
  private readonly options: PendingRefRuntimeOptions;
  private readonly clock: Clock;
  private readonly enabled: boolean;
  private running = false;
  private loopPromise?: Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private resolveSleep?: () => void;

  constructor(
    private readonly unitOfWork: UnitOfWork,
    private readonly submit: SubmitWagerCommands,
    options: PendingReferenceWorkerOptions,
  ) {
    this.options = withDefaults(options);
    this.enabled = options.enabled;
    this.clock = options.clock ?? SYSTEM_CLOCK;
  }

  onModuleInit(): void {
    if (this.enabled) {
      this.start();
    }
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.loopPromise = this.loop();
  }

  /** Shutdown limpo: para o polling e termina o lote em andamento. */
  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
    }
    const wake = this.resolveSleep;
    this.resolveSleep = undefined;
    wake?.();
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  /**
   * Um ciclo: claim (tx curta) → resolve cada linha sequencialmente, cada uma
   * sobre a MESMA tx do use case. Retorno = quantidade claimada (0 = nada vencido).
   */
  async processDueBatch(now = this.clock.now()): Promise<number> {
    const claimed = await this.unitOfWork.run((repositories) =>
      repositories.transactions.claimPendingReference(
        this.options.batchSize,
        now,
        this.options.leaseMs,
      ),
    );
    if (claimed.length === 0) {
      return 0;
    }
    // UPDATE ... RETURNING não garante ordem: processa por created_at para que
    // uma cadeia (ROLLBACK de REFUND pendente) progrida dentro do mesmo batch.
    claimed.sort(
      (a, b) => a.transaction.createdAt.getTime() - b.transaction.createdAt.getTime(),
    );
    for (const row of claimed) {
      // Sequencial: a ordem de claim (created_at) é preservada e erros isolam por linha.
      try {
        const result = await this.submit.execute(this.buildInput(row), {
          pendingResolution: {
            now,
            ttlHours: this.options.ttlHours,
            maxAttempts: this.options.maxAttempts,
          },
        });
        logEvent('info', 'pending_ref.processed', {
          transactionId: row.transaction.id,
          walletId: row.transaction.walletId,
          providerId: row.transaction.providerId,
          correlationId: `pending-${row.transaction.id}`,
          outcome: result.outcome,
          failureCode: result.outcome === 'REJECTED' ? result.failureCode : undefined,
        });
      } catch (error) {
        this.log('error', 'pending_ref.failed', {
          transactionId: row.transaction.id,
          walletId: row.transaction.walletId,
          correlationId: `pending-${row.transaction.id}`,
          error: String(error),
        });
      }
    }
    return claimed.length;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      let claimed: number;
      try {
        claimed = await this.processDueBatch();
      } catch (error) {
        claimed = 0;
        this.log('warn', 'pending_ref.cycle_failed', { error: String(error) });
      }
      if (claimed === 0) {
        await this.sleep(this.options.pollIntervalMs);
      }
    }
  }

  /** Reconstrói o input a partir da própria linha claimada — nunca da memória. */
  private buildInput(row: StoredTransaction): SubmitWagerInput {
    const transaction = row.transaction;
    return {
      idempotencyKey: transaction.idempotencyKey,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      playerId: transaction.playerId,
      walletId: transaction.walletId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      money: transaction.money.toJSON(),
      referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
      correlationId: `pending-${transaction.id}`,
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      this.resolveSleep = resolve;
      this.timer = setTimeout(() => {
        this.resolveSleep = undefined;
        resolve();
      }, ms);
    });
  }

  private log(
    level: 'info' | 'warn' | 'error',
    event: string,
    fields: Record<string, unknown>,
  ): void {
    logEvent(level, event, fields);
  }
}
