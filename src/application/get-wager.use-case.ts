import type { StoredTransaction, UnitOfWork } from './ports';

/** Leituras de transação em autocommit (sem tx, sem persistir nada). */
export class GetWagerTransactionUseCase {
  constructor(private readonly unitOfWork: UnitOfWork) {}

  byId(id: string): Promise<StoredTransaction | undefined> {
    return this.unitOfWork.read((repositories) => repositories.transactions.findById(id));
  }

  byProviderExternal(
    providerId: string,
    externalTransactionId: string,
  ): Promise<StoredTransaction | undefined> {
    return this.unitOfWork.read((repositories) =>
      repositories.transactions.findByProviderExternal(providerId, externalTransactionId),
    );
  }
}
