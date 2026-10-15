import { describe, expect, test } from 'bun:test';

import { CurrencyMismatchError, InvalidMoneyError } from '../../src/domain/errors';
import { Money, type MoneyProps } from '../../src/domain/money';

const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
const usd = (amount: string): Money => Money.from({ amount, currency: 'USD' });

const FIXED_SCALE = /^-?\d+\.\d{2}$/;

describe('Money.from — entradas válidas', () => {
  test('aceita decimal string com escala fixa de 2 e preserva o valor', () => {
    const money = brl('25.00');

    expect(money.currency).toBe('BRL');
    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('round-trip: toJSON devolve o mesmo amount recebido', () => {
    const money = Money.from({ amount: '1000.10', currency: 'BRL' });

    expect(Money.from(money.toJSON()).equals(money)).toBe(true);
  });

  test('rejeita props que não são objeto', () => {
    expect(() => Money.from(null as unknown as MoneyProps)).toThrow(InvalidMoneyError);
    expect(() => Money.from('25.00' as unknown as MoneyProps)).toThrow(InvalidMoneyError);
  });
});

describe('Money.from — entradas inválidas', () => {
  const invalidAmounts: Array<[label: string, amount: unknown]> = [
    ['string vazia', ''],
    ['NaN', 'NaN'],
    ['Infinity', 'Infinity'],
    ['-Infinity', '-Infinity'],
    ['notação científica inteira', '1e3'],
    ['notação científica decimal', '2.5e-1'],
    ['notação científica maiúscula', '1E3'],
    ['mais de 2 casas decimais', '25.123'],
    ['escala de 1 casa', '25.1'],
    ['sem casas decimais', '25'],
    ['valor negativo', '-0.01'],
    ['valor negativo sem centavos', '-5.00'],
    ['texto não numérico', 'abc'],
    ['com espaços', ' 25.00 '],
    ['decimal sem parte inteira', '.50'],
    ['não-string (number)', 25],
    ['não-string (undefined)', undefined],
  ];

  for (const [label, amount] of invalidAmounts) {
    test(`rejeita ${label}: ${JSON.stringify(amount)}`, () => {
      expect(() => Money.from({ amount: amount as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
    });
  }

  test('rejeita currency vazia ou não-string', () => {
    expect(() => Money.from({ amount: '25.00', currency: '' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '25.00', currency: '   ' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '25.00', currency: 42 as unknown as string })).toThrow(InvalidMoneyError);
  });

  test('a rejeição é InvalidMoneyError, não erro genérico de runtime', () => {
    expect(() => brl('1e3')).toThrow(InvalidMoneyError);
    expect(() => brl('-1.00')).toThrow(InvalidMoneyError);
    expect(() => brl('1e3')).not.toThrow(TypeError);
  });
});

describe('Money — operações aritméticas', () => {
  test('add soma mantendo escala fixa de 2', () => {
    expect(brl('25.00').add(brl('10.50')).toJSON()).toEqual({ amount: '35.50', currency: 'BRL' });
  });

  test('subtract subtrai e pode resultar em negativo (resultado, não input)', () => {
    const result = brl('10.00').subtract(brl('25.00'));

    expect(result.toJSON()).toEqual({ amount: '-15.00', currency: 'BRL' });
    expect(result.isNegative()).toBe(true);
  });

  test('aritmética é exata — sem erro de ponto flutuante', () => {
    // 0.10 + 0.20 em number dá 0.30000000000000004; aqui é exato.
    expect(brl('0.10').add(brl('0.20')).toJSON().amount).toBe('0.30');
    // Acima de 2^53 centavos um number perderia precisão.
    expect(brl('90071992547409.93').add(brl('0.01')).toJSON().amount).toBe('90071992547409.94');
  });

  test('negate inverte o sinal e devolve instância nova', () => {
    const original = brl('25.00');
    const negated = original.negate();

    expect(negated.toJSON()).toEqual({ amount: '-25.00', currency: 'BRL' });
    expect(negated.negate().equals(original)).toBe(true);
    expect(original.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('operações nunca serializam notação científica', () => {
    const big = brl('100000000000000000.00');

    expect(big.toString()).not.toContain('e');
    expect(big.add(brl('0.01')).toJSON().amount).not.toContain('e');
    expect(big.toJSON().amount).toBe('100000000000000000.00');
  });

  test('toda serialização respeita a escala fixa de 2', () => {
    const samples = [
      brl('0.10').add(brl('0.20')),
      brl('0.00').subtract(brl('0.01')),
      brl('12345.60').subtract(brl('12345.60')),
      brl('0.05').add(brl('0.05')),
    ];

    for (const money of samples) {
      expect(money.toJSON().amount).toMatch(FIXED_SCALE);
      expect(money.toString().startsWith(money.toJSON().amount)).toBe(true);
    }
  });
});

describe('Money — consultas', () => {
  test('isZero / isPositive / isNegative', () => {
    const negative = brl('0.00').subtract(brl('0.01')); // resultado negativo via operação

    expect(brl('0.00').isZero()).toBe(true);
    expect(brl('0.00').isPositive()).toBe(false);
    expect(brl('0.00').isNegative()).toBe(false);

    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('0.01').isZero()).toBe(false);
    expect(brl('0.01').isNegative()).toBe(false);

    expect(negative.isNegative()).toBe(true);
    expect(negative.isPositive()).toBe(false);
    expect(negative.isZero()).toBe(false);
  });

  test('isLessThan compara valores da mesma moeda', () => {
    expect(brl('10.00').isLessThan(brl('20.00'))).toBe(true);
    expect(brl('20.00').isLessThan(brl('10.00'))).toBe(false);
    expect(brl('10.00').isLessThan(brl('10.00'))).toBe(false);
  });

  test('equals compara valor e moeda', () => {
    expect(brl('25.00').equals(brl('25.00'))).toBe(true);
    expect(brl('25.00').equals(brl('25.01'))).toBe(false);
    expect(brl('25.00').equals(usd('25.00'))).toBe(false);
  });
});

describe('Money — cross-currency', () => {
  test('add lança CurrencyMismatchError entre moedas diferentes', () => {
    expect(() => brl('10.00').add(usd('10.00'))).toThrow(CurrencyMismatchError);
  });

  test('subtract lança CurrencyMismatchError entre moedas diferentes', () => {
    expect(() => brl('10.00').subtract(usd('10.00'))).toThrow(CurrencyMismatchError);
  });

  test('isLessThan lança CurrencyMismatchError entre moedas diferentes', () => {
    expect(() => brl('10.00').isLessThan(usd('10.00'))).toThrow(CurrencyMismatchError);
  });

  test('o erro carrega as duas moedas e mensagem legível', () => {
    try {
      brl('10.00').add(usd('10.00'));
      expect.unreachable('deveria ter lançado CurrencyMismatchError');
    } catch (error) {
      expect(error).toBeInstanceOf(CurrencyMismatchError);
      const mismatch = error as CurrencyMismatchError;
      expect(mismatch.leftCurrency).toBe('BRL');
      expect(mismatch.rightCurrency).toBe('USD');
      expect(mismatch.message).toContain('BRL');
      expect(mismatch.message).toContain('USD');
    }
  });

  test('equals é total para moedas distintas (retorna false, não lança)', () => {
    expect(brl('25.00').equals(usd('25.00'))).toBe(false);
    expect(usd('25.00').equals(brl('25.00'))).toBe(false);
  });
});

describe('Money — imutabilidade', () => {
  test('add retorna nova instância e deixa o original intacto', () => {
    const original = brl('25.00');
    const result = original.add(brl('5.00'));

    expect(result).not.toBe(original);
    expect(result).toBeInstanceOf(Money);
    expect(original.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(result.toJSON()).toEqual({ amount: '30.00', currency: 'BRL' });
  });

  test('subtract retorna nova instância e deixa o original intacto', () => {
    const original = brl('25.00');
    const result = original.subtract(brl('5.00'));

    expect(result).not.toBe(original);
    expect(original.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(result.toJSON()).toEqual({ amount: '20.00', currency: 'BRL' });
  });

  test('negate retorna nova instância e deixa o original intacto', () => {
    const original = brl('25.00');
    const result = original.negate();

    expect(result).not.toBe(original);
    expect(original.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('instância é congelada: tentativa de mutação não altera o estado', () => {
    const money = brl('25.00');

    expect(Object.isFrozen(money)).toBe(true);
    expect(() => {
      (money as { currency: string }).currency = 'USD';
    }).toThrow(TypeError);
    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });
});

describe('Money — zero', () => {
  test('zero cria 0.00 na moeda pedida', () => {
    const zero = Money.zero('BRL');

    expect(zero.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
    expect(zero.isNegative()).toBe(false);
  });

  test('zero participa de operações normalmente', () => {
    expect(Money.zero('BRL').add(brl('25.00')).equals(brl('25.00'))).toBe(true);
    expect(brl('25.00').subtract(Money.zero('BRL')).equals(brl('25.00'))).toBe(true);
  });

  test('negate de zero é canônico: segue 0.00, não -0.00', () => {
    const negatedZero = Money.zero('BRL').negate();

    expect(negatedZero.toJSON().amount).toBe('0.00');
    expect(negatedZero.isZero()).toBe(true);
    expect(negatedZero.isNegative()).toBe(false);
    expect(Money.from(negatedZero.toJSON()).equals(Money.zero('BRL'))).toBe(true);
  });

  test('zero de outra moeda não opera com BRL', () => {
    expect(() => Money.zero('BRL').add(Money.zero('USD'))).toThrow(CurrencyMismatchError);
  });
});

describe('Money — serialização', () => {
  test('toJSON expõe o contrato { amount, currency }', () => {
    expect(brl('25.00').toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('JSON.stringify usa toJSON', () => {
    expect(JSON.stringify(brl('25.00'))).toBe('{"amount":"25.00","currency":"BRL"}');
  });

  test('toString é a representação humana com moeda', () => {
    expect(brl('25.00').toString()).toBe('25.00 BRL');
    expect(brl('0.05').toString()).toBe('0.05 BRL');
    expect(brl('10.00').subtract(brl('25.00')).toString()).toBe('-15.00 BRL');
  });
});
