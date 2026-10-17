import Decimal from 'decimal.js';

import { CurrencyMismatchError, InvalidMoneyError } from './errors';
import { isIso4217CurrencyCode } from './currency-code';

/**
 * Formato de contrato (HTTP/SQS/eventos): decimal string com escala fixa de 2.
 * Ex.: `{ amount: "25.00", currency: "BRL" }`.
 */
export interface MoneyProps {
  amount: string; // decimal string, ex.: "25.00"
  currency: string; // ISO-4217
}

/** Escala fixa: toda entrada e toda serialização usa 2 casas. */
const SCALE = 2;

/** Contrato de entrada: sinal opcional só para categorizar o erro; negativo é rejeitado abaixo. */
const PLAIN_DECIMAL = /^-?(\d+)(\.\d+)?$/;
const SCIENTIFIC_NOTATION = /[eE]/;
const NOT_FINITE = /^[+-]?(nan|infinity)$/i;

function assertValidAmount(amount: unknown): asserts amount is string {
  if (typeof amount !== 'string') {
    throw new InvalidMoneyError(`amount must be a string, received ${amount === null ? 'null' : typeof amount}`);
  }
  if (amount.length === 0) {
    throw new InvalidMoneyError('amount must not be empty');
  }
  if (SCIENTIFIC_NOTATION.test(amount)) {
    throw new InvalidMoneyError(`amount must not use scientific notation: "${amount}"`);
  }
  if (NOT_FINITE.test(amount)) {
    throw new InvalidMoneyError(`amount must be a finite decimal, got "${amount}"`);
  }
  if (!PLAIN_DECIMAL.test(amount)) {
    throw new InvalidMoneyError(`amount must be a plain decimal string: "${amount}"`);
  }
  if (amount.startsWith('-')) {
    throw new InvalidMoneyError(`amount must not be negative in input contracts: "${amount}"`);
  }
  const fraction = amount.slice(amount.indexOf('.') + 1);
  if (amount.indexOf('.') === -1 || fraction.length !== SCALE) {
    throw new InvalidMoneyError(
      `amount must have exactly ${SCALE} decimal places (fixed scale), got "${amount}"`,
    );
  }
}

function assertValidCurrency(currency: unknown): asserts currency is string {
  if (!isIso4217CurrencyCode(currency)) {
    throw new InvalidMoneyError('currency must be an allocated ISO-4217 code');
  }
}

/**
 * Valor monetário imutável com escala fixa de 2 casas.
 *
 * - `amount` é recebido e serializado como string decimal (nunca `number`);
 * - toda operação retorna uma nova instância — o original fica intacto;
 * - operações entre moedas diferentes lançam `CurrencyMismatchError`
 *   (`add`, `subtract`, `isLessThan`); `equals` é total e retorna `false`
 *   para moedas distintas, seguindo o contrato usual de `equals`;
 * - entradas inválidas (`NaN`, `Infinity`, notação científica, vazio,
 *   escala ≠ 2, negativo) são rejeitadas com `InvalidMoneyError`.
 */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    // Imutabilidade em runtime: sem setters e instância congelada.
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (props === null || typeof props !== 'object') {
      throw new InvalidMoneyError('Money props must be an object with amount and currency');
    }
    assertValidAmount(props.amount);
    assertValidCurrency(props.currency);
    return Money.fromValue(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  /** Constrói a partir de um resultado interno já válido (escala assegurada na serialização). */
  private static fromValue(value: Decimal, currency: string): Money {
    // Normaliza -0 (ex.: negate de zero) para 0, mantendo a representação canônica.
    return new Money(value.isZero() ? new Decimal(0) : value, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.fromValue(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.fromValue(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return Money.fromValue(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    if (this.currency !== other.currency) return false;
    return this.value.eq(other.value);
  }

  /** `MoneyProps` pronto para JSON (contrato `{ amount, currency }`). */
  toJSON(): MoneyProps {
    return { amount: this.toStringAmount(), currency: this.currency };
  }

  /** Representação humana estável, ex.: `"25.00 BRL"`. */
  toString(): string {
    return `${this.toStringAmount()} ${this.currency}`;
  }

  private toStringAmount(): string {
    return this.value.toFixed(SCALE);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
