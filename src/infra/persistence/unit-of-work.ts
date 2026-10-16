import { IsolationLevel } from '@mikro-orm/core';
import { MikroORM, type EntityManager } from '@mikro-orm/postgresql';
import { Inject, Injectable } from '@nestjs/common';

import type { Repositories, UnitOfWork } from '../../application/ports';
import { MikroOrmLedgerStore } from './wallet-ledger.store';
import { MikroOrmOutboxStore } from './outbox.store';
import { MikroOrmTransactionStore } from './wager-transaction.store';
import { MikroOrmWalletRepository } from './wallet.repository';

/**
 * Implementa `UnitOfWork` com MikroORM: `run` abre um `em.fork()` e uma tx
 * `READ COMMITTED` com `lock_timeout` de 3s; `read` reusa o fork sem transação.
 * Os repositórios são vinculados ao mesmo `EntityManager` da execução.
 */
@Injectable()
export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}

  run<T>(work: (repositories: Repositories) => Promise<T>): Promise<T> {
    const em = this.orm.em.fork();
    return em.transactional(
      async (tx) => {
        await tx.execute(`SET LOCAL lock_timeout = '3s'`);
        return work(this.buildRepositories(tx));
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );
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
    };
  }
}
