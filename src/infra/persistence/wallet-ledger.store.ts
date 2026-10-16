import type { EntityManager } from '@mikro-orm/postgresql';

import type { LedgerStore } from '../../application/ports';
import type { WalletLedgerEntry } from '../../domain/wallet-ledger-entry';

/** INSERT puro — a imutabilidade é do trigger no banco; aqui nunca há UPDATE/DELETE. */
export class MikroOrmLedgerStore implements LedgerStore {
  constructor(private readonly em: EntityManager) {}

  async insert(entry: WalletLedgerEntry): Promise<void> {
    await this.em.execute(
      `INSERT INTO wallet_ledger_entry
         (id, wallet_id, transaction_id, direction, amount, currency,
          balance_before_amount, balance_before_currency,
          balance_after_amount, balance_after_currency, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.toJSON().amount,
        entry.money.currency,
        entry.balanceBefore.toJSON().amount,
        entry.balanceBefore.currency,
        entry.balanceAfter.toJSON().amount,
        entry.balanceAfter.currency,
        entry.createdAt,
      ],
    );
  }
}
