/**
 * Erros de domínio — só o que o `Money` precisa nesta fatia.
 * Sem dependência de Nest/ORM: o mapeamento para HTTP/SQS acontece na borda.
 */

/** Entrada inválida em contrato de `Money` (formato de amount/currency). */
export class InvalidMoneyError extends Error {
  readonly code = 'INVALID_MONEY' as const;

  constructor(message: string) {
    super(message);
    this.name = 'InvalidMoneyError';
  }
}

/** Operação entre valores de moedas diferentes. */
export class CurrencyMismatchError extends Error {
  readonly code = 'CURRENCY_MISMATCH' as const;

  constructor(
    readonly leftCurrency: string,
    readonly rightCurrency: string,
  ) {
    super(`Currency mismatch: "${leftCurrency}" !== "${rightCurrency}"`);
    this.name = 'CurrencyMismatchError';
  }
}
