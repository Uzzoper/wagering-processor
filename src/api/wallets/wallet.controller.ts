import { Body, Controller, Get, HttpCode, Param, Post, Query } from '@nestjs/common';

import { CreateWalletUseCase } from '../../application/create-wallet.use-case';
import { GetWalletUseCase } from '../../application/get-wallet.use-case';
import { ListLedgerUseCase } from '../../application/list-ledger.use-case';
import type { LedgerEntryRow } from '../../application/ports';
import { ReconcileWalletUseCase, type ReconciliationSnapshot } from '../../application/reconcile-wallet.use-case';
import type { MoneyProps } from '../../domain/money';
import { LedgerDirection } from '../../domain/wallet-ledger-entry';
import type { Wallet } from '../../domain/wallet';
import { FailureCodes } from '../../domain/failure-code';
import { BusinessError } from '../errors/business-error';
import type { CreateWalletBody } from './create-wallet.dto';
import { isUuid } from './create-wallet.dto';
import { CreateWalletPipe } from './create-wallet.pipe';
import type { LedgerResponse, ReconciliationResponse } from './ledger.dto';
import { parseLedgerQuery } from './ledger.dto';

export interface WalletResponse {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

/**
 * Endpoints de wallet. Sem auth (deferida globalmente — §6): abertos.
 * `POST` mapeia `WALLET_CONFLICT` para `409`; `GET` inexistente para `404`;
 * falha de `limit`/`cursor` malformados para `400`.
 */
@Controller('wallets')
export class WalletController {
  constructor(
    private readonly createWallet: CreateWalletUseCase,
    private readonly getWallet: GetWalletUseCase,
    private readonly listLedger: ListLedgerUseCase,
    private readonly reconcileWallet: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body(CreateWalletPipe) body: CreateWalletBody): Promise<WalletResponse> {
    const result = await this.createWallet.execute(body);
    if (result.outcome === 'WALLET_CONFLICT') {
      throw new BusinessError(
        FailureCodes.WalletConflict,
        `wallet already exists for playerId ${body.playerId} and currency ${body.initialBalance.currency}`,
      );
    }
    return toResponse(result.wallet);
  }

  @Get(':walletId')
  async getById(@Param('walletId') walletId: string): Promise<WalletResponse> {
    if (!isUuid(walletId)) {
      throw new BusinessError(FailureCodes.InvalidPayload, 'walletId must be a valid UUID');
    }

    const wallet = await this.getWallet.execute(walletId);
    if (wallet === undefined) {
      throw new BusinessError(FailureCodes.WalletNotFound, `wallet "${walletId}" not found`);
    }

    return toResponse(wallet);
  }

  @Get(':walletId/ledger')
  async getLedger(
    @Param('walletId') walletId: string,
    @Query() query: Record<string, unknown>,
  ): Promise<LedgerResponse> {
    if (!isUuid(walletId)) {
      throw new BusinessError(FailureCodes.InvalidPayload, 'walletId must be a valid UUID');
    }

    const parsed = parseLedgerQuery(query);
    const page = await this.listLedger.execute(walletId, parsed);
    if (page === undefined) {
      throw new BusinessError(FailureCodes.WalletNotFound, `wallet "${walletId}" not found`);
    }

    return toLedgerResponse(page);
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  async reconcile(
    @Param('walletId') walletId: string,
  ): Promise<ReconciliationResponse> {
    if (!isUuid(walletId)) {
      throw new BusinessError(FailureCodes.InvalidPayload, 'walletId must be a valid UUID');
    }

    const snapshot = await this.reconcileWallet.execute(walletId);
    if (snapshot === undefined) {
      throw new BusinessError(FailureCodes.WalletNotFound, `wallet "${walletId}" not found`);
    }

    return toReconciliationResponse(snapshot);
  }
}

function toResponse(wallet: Wallet): WalletResponse {
  return {
    id: wallet.id,
    playerId: wallet.playerId,
    balance: wallet.balance.toJSON(),
    version: wallet.version,
  };
}

function toLedgerResponse(page: {
  items: LedgerEntryRow[];
  nextCursor: string | null;
}): LedgerResponse {
  return {
    items: page.items.map((item) => ({
      id: item.id,
      transactionId: item.transactionId,
      direction: item.direction === LedgerDirection.Debit ? 'DEBIT' : 'CREDIT',
      money: item.money.toJSON(),
      balanceBefore: item.balanceBefore.toJSON(),
      balanceAfter: item.balanceAfter.toJSON(),
      createdAt: item.createdAt.toISOString(),
    })),
    nextCursor: page.nextCursor,
  };
}

function toReconciliationResponse(snapshot: ReconciliationSnapshot): ReconciliationResponse {
  return {
    walletId: snapshot.walletId,
    storedBalance: snapshot.storedBalance,
    calculatedBalance: snapshot.calculatedBalance,
    difference: snapshot.difference,
    consistent: snapshot.consistent,
    checkedEntries: snapshot.checkedEntries,
  };
}
