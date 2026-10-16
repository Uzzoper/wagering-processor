import type { Wallet } from '../domain/wallet';
import type { UnitOfWork } from './ports';

/** Consulta de wallet por id — leitura pura, sem persistir nada. */
export class GetWalletUseCase {
  constructor(private readonly unitOfWork: UnitOfWork) {}

  execute(walletId: string): Promise<Wallet | undefined> {
    return this.unitOfWork.read(({ wallets }) => wallets.findById(walletId));
  }
}
