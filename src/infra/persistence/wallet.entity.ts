import { defineEntity } from '@mikro-orm/core';

/**
 * Espelho fino da tabela `wallet`. Só colunas + índices — nenhuma regra de
 * negócio (essa vive em `src/domain/wallet.ts`). MikroORM v7 não usa decorators;
 * a entidade é definida com `defineEntity`.
 */
export const WalletEntity = defineEntity({
  name: 'Wallet',
  tableName: 'wallet',
  properties: (p) => ({
    id: p.uuid().primary(),
    playerId: p.uuid().fieldName('player_id'),
    currency: p.string().length(3),
    balanceAmount: p.decimal().precision(19).scale(2).fieldName('balance_amount'),
    balanceCurrency: p.string().length(3).fieldName('balance_currency'),
    version: p.integer(),
    createdAt: p.datetime().fieldName('created_at'),
    updatedAt: p.datetime().fieldName('updated_at'),
  }),
  uniques: [{ name: 'uq_wallet_player_currency', properties: ['playerId', 'currency'] }],
});
