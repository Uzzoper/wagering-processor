import type { MoneyProps } from '../domain/money';
import {
  RECONCILIATION_MISMATCHES_METRIC,
  type MetricsPort,
  type WalletReadStore,
} from './ports';

/** Retrato final da reconciliação — o corpo da resposta é exatamente este shape. */
export interface ReconciliationSnapshot {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  /** `stored − calculated`: string de 2 casas, pode ser negativa (ex. "-3.50"). */
  difference: string;
  consistent: boolean;
  checkedEntries: number;
}

/**
 * Reconcilia o saldo materializado com a soma do ledger (fatia 8a).
 *
 * Divergências NUNCA são corrigidas aqui. O retrato vem de UMA query SQL
 * (uma única instrução em READ COMMITTED já é um snapshot consistente — sem
 * lock da wallet quente e sem a corrida de ler wallet e ledger em dois passos).
 * Em divergência: log de erro JSON + contador; o corpo mantém `consistent: false`.
 *
 * `difference` NÃO passa por `Money.from` de propósito: a diferença pode ser
 * negativa e o contrato de entrada rejeita negativo; o NUMERIC do Postgres já
 * sai como texto de 2 casas (nunca float).
 */
export class ReconcileWalletUseCase {
  constructor(
    private readonly readStore: WalletReadStore,
    private readonly metrics: MetricsPort,
  ) {}

  async execute(walletId: string): Promise<ReconciliationSnapshot | undefined> {
    const snapshot = await this.readStore.reconcile(walletId);
    if (snapshot === undefined) {
      return undefined;
    }

    // NUMERIC(19,2) do PG sai sempre com escala 2, então o texto cobre o "zero".
    const consistent = snapshot.difference === '0.00';
    if (!consistent) {
      this.metrics.increment(RECONCILIATION_MISMATCHES_METRIC);
      // Aplicação é pura: sem import de infra — console direto no formato da casa.
      // Sem saldos monetários (§12): o log só identifica a wallet e a divergência.
      console.error(
        JSON.stringify({
          level: 'error',
          event: 'reconciliation.mismatch',
          walletId,
          difference: snapshot.difference,
        }),
      );
    }

    return {
      walletId,
      storedBalance: snapshot.storedBalance.toJSON(),
      calculatedBalance: {
        // Saída sem revalidação de entrada: o cálculo pode divergir para
        // qualquer sinal num cenário patológico e não deve estourar o parse.
        amount: snapshot.calculatedAmount,
        currency: snapshot.storedBalance.currency,
      },
      difference: snapshot.difference,
      consistent,
      checkedEntries: snapshot.checkedEntries,
    };
  }
}
