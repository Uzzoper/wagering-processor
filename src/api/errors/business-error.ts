import type { FailureCode } from '../../domain/failure-code';

/**
 * Erro de borda que carrega um `failureCode` do §7 do ARCHITECTURE.
 * O filtro único traduz para o status HTTP — o domínio nunca conhece HTTP.
 */
export class BusinessError extends Error {
  constructor(
    readonly failureCode: FailureCode,
    message: string,
  ) {
    super(message);
    this.name = 'BusinessError';
  }
}
