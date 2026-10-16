/**
 * Taxonomia travada em ARCHITECTURE.md §7. Sem mapeamento para HTTP/SQS
 * (isso é da borda `api/`): erros de domínio **não** carregam `failureCode`;
 * os use cases traduzem a violação para um destes códigos.
 */
export const FailureCodes = {
  InsufficientBalance: 'INSUFFICIENT_BALANCE',
  ReversalWouldOverdraw: 'REVERSAL_WOULD_OVERDRAW',
  ReferenceNotFound: 'REFERENCE_NOT_FOUND',
  InvalidReference: 'INVALID_REFERENCE',
  AlreadyReversed: 'ALREADY_REVERSED',
  AmountMismatch: 'AMOUNT_MISMATCH',
  CurrencyMismatch: 'CURRENCY_MISMATCH',
  WalletOwnerMismatch: 'WALLET_OWNER_MISMATCH',
  IdempotencyConflict: 'IDEMPOTENCY_CONFLICT',
  WalletConflict: 'WALLET_CONFLICT',
  WalletNotFound: 'WALLET_NOT_FOUND',
  InvalidPayload: 'INVALID_PAYLOAD',
  PendingReference: 'PENDING_REFERENCE',
  InfraTransient: 'INFRA_TRANSIENT',
} as const;

export type FailureCode = (typeof FailureCodes)[keyof typeof FailureCodes];
