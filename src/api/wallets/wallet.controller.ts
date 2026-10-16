import { Body, Controller, Get, HttpCode, Param, Post } from '@nestjs/common';

import { CreateWalletUseCase } from '../../application/create-wallet.use-case';
import { GetWalletUseCase } from '../../application/get-wallet.use-case';
import type { MoneyProps } from '../../domain/money';
import type { Wallet } from '../../domain/wallet';
import { FailureCodes } from '../../domain/failure-code';
import { BusinessError } from '../errors/business-error';
import type { CreateWalletBody } from './create-wallet.dto';
import { isUuid } from './create-wallet.dto';
import { CreateWalletPipe } from './create-wallet.pipe';

export interface WalletResponse {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

/**
 * Endpoints de wallet. Sem auth (deferida globalmente — §6): abertos.
 * `POST` mapeia `WALLET_CONFLICT` para `409`; `GET` inexistente para `404`.
 */
@Controller('wallets')
export class WalletController {
  constructor(
    private readonly createWallet: CreateWalletUseCase,
    private readonly getWallet: GetWalletUseCase,
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
}

function toResponse(wallet: Wallet): WalletResponse {
  return {
    id: wallet.id,
    playerId: wallet.playerId,
    balance: wallet.balance.toJSON(),
    version: wallet.version,
  };
}
