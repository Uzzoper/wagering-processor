import { Module } from '@nestjs/common';

import { CreateWalletUseCase } from '../../application/create-wallet.use-case';
import { GetWalletUseCase } from '../../application/get-wallet.use-case';
import { ListLedgerUseCase } from '../../application/list-ledger.use-case';
import { ReconcileWalletUseCase } from '../../application/reconcile-wallet.use-case';
import type { MetricsPort, UnitOfWork, WalletReadStore } from '../../application/ports';
import { METRICS, UNIT_OF_WORK, WALLET_READ_STORE } from '../../application/ports';
import { WalletController } from './wallet.controller';

/**
 * Use cases são classes puras (sem decorators) ligadas por fábrica; o
 * `UnitOfWork`, o `WalletReadStore` e o `MetricsPort` vêm dos módulos globais
 * de infra, então a api não importa infra diretamente.
 */
@Module({
  controllers: [WalletController],
  providers: [
    {
      provide: CreateWalletUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new CreateWalletUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
    {
      provide: GetWalletUseCase,
      useFactory: (unitOfWork: UnitOfWork) => new GetWalletUseCase(unitOfWork),
      inject: [UNIT_OF_WORK],
    },
    {
      provide: ListLedgerUseCase,
      useFactory: (readStore: WalletReadStore) => new ListLedgerUseCase(readStore),
      inject: [WALLET_READ_STORE],
    },
    {
      provide: ReconcileWalletUseCase,
      useFactory: (readStore: WalletReadStore, metrics: MetricsPort) =>
        new ReconcileWalletUseCase(readStore, metrics),
      inject: [WALLET_READ_STORE, METRICS],
    },
  ],
})
export class WalletModule {}
