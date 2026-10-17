import { IsolationLevel } from '@mikro-orm/core';
import { MikroORM, type EntityManager } from '@mikro-orm/postgresql';
import { Inject, Injectable, Optional } from '@nestjs/common';

import type { MetricsPort, Repositories, UnitOfWork } from '../../application/ports';
import { METRICS } from '../../application/ports';
import { DB_LOCK_CONFLICTS_TOTAL } from '../../application/metric-names';
import { MikroOrmInboxStore } from './inbox.store';
import { MikroOrmLedgerStore } from './wallet-ledger.store';
import { MikroOrmOutboxStore } from './outbox.store';
import { MikroOrmTransactionStore } from './wager-transaction.store';
import { MikroOrmWalletRepository } from './wallet.repository';

/** `SET LOCAL lock_timeout` aceita inteiro em ms — default trava nos 3s do §11. */
const DEFAULT_LOCK_TIMEOUT_MS = 3_000;

/** Token DI das opções da UnitOfWork (lem lockTimeoutMs + métricas). */
export const UNIT_OF_WORK_OPTIONS = Symbol('MikroOrmUnitOfWorkOptions');

export interface MikroOrmUnitOfWorkOptions {
  /** Prazo do lock da carteira; testes de lock usam valores baixos. */
  lockTimeoutMs?: number;
  /** Registry opcional: fora do DI (testes que constroem direto) não instrumenta. */
  metrics?: MetricsPort;
}

/**
 * Implementa `UnitOfWork` com MikroORM: `run` abre um `em.fork()` e uma tx
 * `READ COMMITTED` com `lock_timeout` configurável (default 3s); `read` reusa
 * o fork sem transação. Os repositórios são vinculados ao mesmo
 * `EntityManager` da execução. Conflito de lock (55P03/40P01) é contado e
 * rethrowado — a borda mapeia para `503 INFRA_TRANSIENT` (nunca vira REJECTED).
 */
@Injectable()
export class MikroOrmUnitOfWork implements UnitOfWork {
  private readonly lockTimeoutMs: number;
  private readonly metrics?: MetricsPort;

  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Optional() @Inject(UNIT_OF_WORK_OPTIONS) options?: MikroOrmUnitOfWorkOptions,
  ) {
    this.lockTimeoutMs = options?.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS;
    this.metrics = options?.metrics;
  }

  async run<T>(work: (repositories: Repositories) => Promise<T>): Promise<T> {
    const em = this.orm.em.fork();
    try {
      // `await` dentro do try: rejeições da tx passam pelo catch do lock abaixo.
      return await em.transactional(
        async (tx) => {
          await tx.execute(`SET LOCAL lock_timeout = ${this.lockTimeoutMs}`);
          return work(this.buildRepositories(tx));
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
    } catch (error) {
      if (isLockConflict(error)) {
        this.metrics?.increment(DB_LOCK_CONFLICTS_TOTAL);
      }
      throw error;
    }
  }

  read<T>(work: (repositories: Repositories) => Promise<T>): Promise<T> {
    return work(this.buildRepositories(this.orm.em.fork()));
  }

  private buildRepositories(em: EntityManager): Repositories {
    return {
      wallets: new MikroOrmWalletRepository(em),
      transactions: new MikroOrmTransactionStore(em),
      ledger: new MikroOrmLedgerStore(em),
      outbox: new MikroOrmOutboxStore(em),
      inbox: new MikroOrmInboxStore(em),
    };
  }
}

function isLockConflict(error: unknown): boolean {
  const code = (error as { code?: string } | undefined)?.code;
  return code === '55P03' || code === '40P01';
}
