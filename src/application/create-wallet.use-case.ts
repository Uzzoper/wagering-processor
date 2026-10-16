import { createHash } from 'node:crypto';

import type { MoneyProps } from '../domain/money';
import { Money } from '../domain/money';
import { WalletBalanceChanged, WagerTransactionProcessed } from '../domain/events';
import { LedgerDirection, WalletLedgerEntry } from '../domain/wallet-ledger-entry';
import { WagerTransaction, WagerTransactionKind } from '../domain/wager-transaction';
import { Wallet } from '../domain/wallet';
import type { UnitOfWork } from './ports';

/** Provider interno da transação de abertura; nunca submetido pela API/fila. */
export const SYSTEM_PROVIDER_ID = 'system';
const OPENING_ROUND_ID = 'opening';
const OPENING_GAME_ID = 'opening';

export interface CreateWalletInput {
  playerId: string;
  initialBalance: MoneyProps;
  /** Propagado para os eventos; default gerado aqui quando ausente. */
  correlationId?: string;
}

export interface CreateWalletCreated {
  outcome: 'CREATED';
  wallet: Wallet;
}

export interface CreateWalletConflict {
  outcome: 'WALLET_CONFLICT';
}

export type CreateWalletResult = CreateWalletCreated | CreateWalletConflict;

/** SHA-256 do payload canônico da abertura — preenche `payload_hash` (NOT NULL, varchar 64). */
function openingPayloadHash(wallet: Wallet): string {
  const canonical = JSON.stringify({
    providerId: SYSTEM_PROVIDER_ID,
    externalTransactionId: `opening:${wallet.id}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: OPENING_ROUND_ID,
    gameId: OPENING_GAME_ID,
    kind: WagerTransactionKind.Opening,
    amount: wallet.balance.toJSON().amount,
    currency: wallet.currency,
  });
  return createHash('sha256').update(canonical).digest('hex');
}

/**
 * Cria a wallet. `playerId+currency` duplicado RETORNA `WALLET_CONFLICT` (nunca lança):
 * a inserção usa `ON CONFLICT DO NOTHING RETURNING` e não capturamos `23505` (abortaria a tx).
 * Saldo > 0 gera `OPENING` PROCESSED + 1 lançamento `CREDIT` + 2 eventos, tudo na mesma tx.
 */
export class CreateWalletUseCase {
  constructor(private readonly unitOfWork: UnitOfWork) {}

  async execute(input: CreateWalletInput): Promise<CreateWalletResult> {
    const initialBalance = Money.from(input.initialBalance);
    const now = new Date();
    const correlationId = input.correlationId ?? crypto.randomUUID();
    // `open` já nasce com `version = 1`; o saldo inicial não passa por `credit()`.
    const wallet = Wallet.open({
      id: crypto.randomUUID(),
      playerId: input.playerId,
      initialBalance,
    });

    return this.unitOfWork.run(async (repositories): Promise<CreateWalletResult> => {
      const inserted = await repositories.wallets.insertIfAbsent(wallet);
      if (!inserted) {
        return { outcome: 'WALLET_CONFLICT' };
      }

      if (initialBalance.isPositive()) {
        const transaction = buildOpeningTransaction(wallet, now);
        const entry = WalletLedgerEntry.create({
          id: crypto.randomUUID(),
          walletId: wallet.id,
          transactionId: transaction.id,
          direction: LedgerDirection.Credit,
          money: initialBalance,
          balanceBefore: Money.zero(initialBalance.currency),
          balanceAfter: initialBalance,
          createdAt: now,
        });

        await repositories.transactions.insert(transaction, initialBalance);
        await repositories.ledger.insert(entry);
        await repositories.outbox.insert([
          WagerTransactionProcessed.from(transaction, initialBalance, { correlationId }),
          WalletBalanceChanged.from(wallet, entry, { correlationId }),
        ]);
      }

      return { outcome: 'CREATED', wallet };
    });
  }
}

function buildOpeningTransaction(wallet: Wallet, at: Date): WagerTransaction {
  const transaction = WagerTransaction.create({
    id: crypto.randomUUID(),
    providerId: SYSTEM_PROVIDER_ID,
    externalTransactionId: `opening:${wallet.id}`,
    idempotencyKey: `opening:${wallet.id}`,
    payloadHash: openingPayloadHash(wallet),
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: OPENING_ROUND_ID,
    gameId: OPENING_GAME_ID,
    kind: WagerTransactionKind.Opening,
    money: wallet.balance,
    createdAt: at,
  });
  transaction.markProcessed(undefined, at);
  return transaction;
}
