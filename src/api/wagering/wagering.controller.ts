import { Body, Controller, Get, Headers, Param, Post, Req, Res } from '@nestjs/common';

import type { StoredTransaction } from '../../application/ports';
import {
  SubmitWagerTransactionUseCase,
  type SubmitPendingReference,
  type SubmitProcessed,
  type SubmitRejected,
} from '../../application/submit-wager.use-case';
import { GetWagerTransactionUseCase } from '../../application/get-wager.use-case';
import { FailureCodes } from '../../domain/failure-code';
import type { MoneyProps } from '../../domain/money';
import { BusinessError } from '../errors/business-error';
import { isUuid } from '../wallets/create-wallet.dto';
import type { SubmitWagerBody } from './submit-wager.dto';
import { SubmitWagerPipe } from './submit-wager.pipe';

export interface SubmitWagerResponse {
  transactionId: string;
  status: string;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: string;
}

export interface WagerTransactionEnvelope {
  transactionId: string;
  status: string;
  balance: MoneyProps;
  failureCode?: string;
}

/** Mínimo do Express usado aqui; evita depender dos tipos do framework. */
interface StatusResponse {
  status(code: number): unknown;
}

/** Request mínimo do middleware de correlação (veja `correlationMiddleware`). */
interface CorrelatedRequest {
  correlationId?: string;
}

/**
 * Endpoints de wagering. Sem auth (deferida globalmente — §6): abertos.
 * Sucesso com status variável (201/200/202) via `passthrough`; negócio
 * rejeitado/ausente/conflito vira `BusinessError` (filtro único mapeia).
 */
@Controller()
export class WageringController {
  constructor(
    private readonly submit: SubmitWagerTransactionUseCase,
    private readonly queries: GetWagerTransactionUseCase,
  ) {}

  @Post('wagering/transactions')
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKey: string | string[] | undefined,
    @Body(SubmitWagerPipe) body: SubmitWagerBody,
    @Req() request: CorrelatedRequest,
    @Res({ passthrough: true }) res: StatusResponse,
  ): Promise<SubmitWagerResponse> {
    const key = Array.isArray(idempotencyKey) ? idempotencyKey[0] : idempotencyKey;
    if (typeof key !== 'string' || key.trim().length === 0) {
      throw new BusinessError(FailureCodes.InvalidPayload, 'Idempotency-Key header is required');
    }

    const result = await this.submit.execute({
      idempotencyKey: key,
      providerId: body.providerId,
      externalTransactionId: body.externalTransactionId,
      playerId: body.playerId,
      walletId: body.walletId,
      roundId: body.roundId,
      gameId: body.gameId,
      kind: body.kind,
      money: body.money,
      referenceExternalTransactionId: body.referenceExternalTransactionId,
      // Borda → use case: o X-Correlation-Id do request (propagado p/ outbox).
      correlationId: request.correlationId,
    });

    switch (result.outcome) {
      case 'PROCESSED': {
        res.status(result.replay ? 200 : 201);
        return toSubmitResponse(result);
      }
      case 'REJECTED': {
        res.status(422);
        return { ...toSubmitResponse(result), failureCode: result.failureCode };
      }
      case 'PENDING_REFERENCE': {
        res.status(202);
        return toSubmitResponse(result);
      }
      case 'WALLET_NOT_FOUND':
        throw new BusinessError(
          FailureCodes.WalletNotFound,
          `wallet "${body.walletId}" not found`,
        );
      case 'IDEMPOTENCY_CONFLICT':
        throw new BusinessError(
          FailureCodes.IdempotencyConflict,
          `idempotency key "${key}" was already used with a different payload`,
        );
      case 'INBOX_DUPLICATE':
        throw new Error('Inbox duplicate outcome is only valid for SQS submissions');
    }
  }

  @Get('wagering/transactions/:transactionId')
  async getById(@Param('transactionId') transactionId: string): Promise<WagerTransactionEnvelope> {
    if (!isUuid(transactionId)) {
      throw new BusinessError(FailureCodes.InvalidPayload, 'transactionId must be a valid UUID');
    }
    const stored = await this.queries.byId(transactionId);
    if (stored === undefined) {
      throw new BusinessError(
        FailureCodes.TransactionNotFound,
        `transaction "${transactionId}" not found`,
      );
    }
    return toEnvelope(stored);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByProviderExternal(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionEnvelope> {
    // Ids externos são opacos do provedor (ex.: `tx-123`): sem formato a validar.
    const stored = await this.queries.byProviderExternal(providerId, externalTransactionId);
    if (stored === undefined) {
      throw new BusinessError(
        FailureCodes.TransactionNotFound,
        `transaction "${providerId}:${externalTransactionId}" not found`,
      );
    }
    return toEnvelope(stored);
  }
}

function toSubmitResponse(
  result: SubmitProcessed | SubmitRejected | SubmitPendingReference,
): SubmitWagerResponse {
  return {
    transactionId: result.transaction.id,
    status: result.transaction.status,
    balance: result.observedBalance.toJSON(),
    idempotentReplay: result.replay,
  };
}

function toEnvelope(stored: StoredTransaction): WagerTransactionEnvelope {
  const { transaction, observedBalance } = stored;
  return {
    transactionId: transaction.id,
    status: transaction.status,
    balance: observedBalance.toJSON(),
    ...(transaction.failureCode !== undefined ? { failureCode: transaction.failureCode } : {}),
  };
}
