import { defineEntity } from '@mikro-orm/core';

/**
 * Espelho fino da tabela `wallet_ledger_entry`. Só colunas + índices.
 * A imutabilidade é garantida pelo trigger `trg_ledger_immutable` (migração) e
 * pelo domínio; aqui não há métodos de transição.
 */
export const WalletLedgerEntryEntity = defineEntity({
  name: 'WalletLedgerEntry',
  tableName: 'wallet_ledger_entry',
  properties: (p) => ({
    id: p.uuid().primary(),
    walletId: p.uuid().fieldName('wallet_id'),
    transactionId: p.uuid().fieldName('transaction_id'),
    direction: p.string().length(6),
    amount: p.decimal().precision(19).scale(2),
    currency: p.string().length(3),
    balanceBeforeAmount: p.decimal().precision(19).scale(2).fieldName('balance_before_amount'),
    balanceBeforeCurrency: p.string().length(3).fieldName('balance_before_currency'),
    balanceAfterAmount: p.decimal().precision(19).scale(2).fieldName('balance_after_amount'),
    balanceAfterCurrency: p.string().length(3).fieldName('balance_after_currency'),
    createdAt: p.datetime().fieldName('created_at'),
  }),
  uniques: [{ name: 'uq_ledger_tx_wallet', properties: ['transactionId', 'walletId'] }],
  indexes: [{ name: 'ix_ledger_wallet_id', properties: ['walletId', 'id'] }],
});
