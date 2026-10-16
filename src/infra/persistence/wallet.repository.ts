import type { EntityManager } from '@mikro-orm/postgresql';

import type { WalletRepository } from '../../application/ports';
import { Money } from '../../domain/money';
import { Wallet } from '../../domain/wallet';
import { WalletEntity } from './wallet.entity';

/**
 * Repositório de wallet ligado a um `EntityManager` (e à tx corrente).
 * Escrita usa `ON CONFLICT ... DO NOTHING RETURNING` (nunca captura `23505`);
 * leitura reconstrói o domínio via `Wallet.rehydrate`.
 */
export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async insertIfAbsent(wallet: Wallet): Promise<boolean> {
    const rows = await this.em.execute<Array<{ id: string }>>(
      `INSERT INTO wallet
         (id, player_id, currency, balance_amount, balance_currency, version, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (player_id, currency) DO NOTHING
       RETURNING id`,
      [
        wallet.id,
        wallet.playerId,
        wallet.currency,
        wallet.balance.toJSON().amount,
        wallet.currency,
        wallet.version,
        wallet.createdAt,
        wallet.updatedAt,
      ],
    );
    return rows.length > 0;
  }

  async findById(id: string): Promise<Wallet | undefined> {
    const entity = await this.em.findOne(WalletEntity, { id });
    if (entity === null) {
      return undefined;
    }
    return Wallet.rehydrate({
      id: entity.id,
      playerId: entity.playerId,
      currency: entity.currency,
      balance: Money.from({ amount: entity.balanceAmount, currency: entity.balanceCurrency }),
      version: entity.version,
      createdAt: entity.createdAt,
      updatedAt: entity.updatedAt,
    });
  }

  /** `SELECT ... FOR UPDATE` na mesma tx — serializa a concorrência por `walletId`. */
  async findByIdForUpdate(id: string): Promise<Wallet | undefined> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT id, player_id, currency, balance_amount, balance_currency,
              version, created_at, updated_at
         FROM wallet WHERE id = ? FOR UPDATE`,
      [id],
    );
    const row = rows[0];
    if (row === undefined) {
      return undefined;
    }
    return Wallet.rehydrate({
      id: row.id as string,
      playerId: row.player_id as string,
      currency: row.currency as string,
      balance: Money.from({
        amount: row.balance_amount as string,
        currency: row.balance_currency as string,
      }),
      version: row.version as number,
      createdAt: row.created_at as Date,
      updatedAt: row.updated_at as Date,
    });
  }

  /** UPDATE direto; o chamador já segurou o lock e validou as invariantes no domínio. */
  async update(wallet: Wallet): Promise<void> {
    await this.em.execute(
      `UPDATE wallet
          SET balance_amount = ?, balance_currency = ?, version = ?, updated_at = ?
        WHERE id = ?`,
      [
        wallet.balance.toJSON().amount,
        wallet.currency,
        wallet.version,
        wallet.updatedAt,
        wallet.id,
      ],
    );
  }
}
