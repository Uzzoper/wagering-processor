import { MikroORM } from '@mikro-orm/postgresql';

import type {
  LedgerEntryRow,
  LedgerPage,
  WalletReconciliationData,
  WalletReadStore,
} from '../../application/ports';
import type { LedgerCursor } from '../../application/ledger-cursor';
import { Money } from '../../domain/money';
import { LedgerDirection } from '../../domain/wallet-ledger-entry';

interface LedgerRowShape {
  wallet_id: string | null;
  id: string | null;
  transaction_id: string | null;
  direction: string | null;
  amount: string | null;
  currency: string | null;
  balance_before_amount: string | null;
  balance_before_currency: string | null;
  balance_after_amount: string | null;
  balance_after_currency: string | null;
  created_at_iso: string | null;
}

/**
 * Leituras de wallet/ledger FORA da unidade de trabalho (autocommit, um
 * `em.fork()` por chamada) — paginação e retratos não persistem nada.
 */
export class MikroOrmWalletReadStore implements WalletReadStore {
  constructor(private readonly orm: MikroORM) {}

  async ledgerPage(
    walletId: string,
    cursor: LedgerCursor | undefined,
    limit: number,
  ): Promise<LedgerPage | undefined> {
    const em = this.orm.em.fork();
    // LEFT JOIN a partir da wallet: existência e página saem da MESMA query.
    // O filtro keyset fica no ON para a wallet ainda gerar a linha nula.
    const params: unknown[] = [];
    let keyset = '';
    if (cursor !== undefined) {
      // `::timestamptz` preserva microssegundos da string ISO do cursor.
      keyset = ' AND (l.created_at, l.id) > (?::timestamptz, ?)';
      params.push(cursor.createdAt, cursor.id);
    }
    params.push(walletId, limit + 1);
    const rows = await em.execute<Array<LedgerRowShape>>(
      `SELECT w.id                       AS wallet_id,
              l.id                       AS id,
              l.transaction_id           AS transaction_id,
              l.direction                AS direction,
              l.amount                   AS amount,
              l.currency                 AS currency,
              l.balance_before_amount    AS balance_before_amount,
              l.balance_before_currency  AS balance_before_currency,
              l.balance_after_amount     AS balance_after_amount,
              l.balance_after_currency   AS balance_after_currency,
              to_char(l.created_at AT TIME ZONE 'UTC',
                      'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at_iso
         FROM wallet w
         LEFT JOIN wallet_ledger_entry l
                ON l.wallet_id = w.id${keyset}
        WHERE w.id = ?
        ORDER BY l.created_at ASC, l.id ASC
        LIMIT ?`,
      params,
    );
    const first = rows[0];
    if (first === undefined || first.wallet_id === null) {
      // Linha nula do LEFT JOIN = wallet inexistente.
      return undefined;
    }
    const kept = rows.filter((row) => row.id !== null);
    // Look-ahead: linha extra além do limit → há próxima página; o cursor
    // leva o ISO (µs) da ÚLTIMA linha devolvida (Date perde 3 dígitos).
    const hasMore = kept.length > limit;
    const items = kept.slice(0, limit);
    const boundary = items[items.length - 1];
    return {
      items: items.map(toEntryRow),
      cursorCreatedAt:
        hasMore && boundary !== undefined ? (boundary as { created_at_iso: string }).created_at_iso : undefined,
    };
  }

  async reconcile(walletId: string): Promise<WalletReconciliationData | undefined> {
    const em = this.orm.em.fork();
    // A diferença é calculada no SQL (NUMERIC: exato, tipo pode ser negativo);
    // wallet sem lançamento → COALESCE zera soma e mantém a linha da wallet.
    const rows = await em.execute<Array<Record<string, unknown>>>(
      `SELECT w.balance_amount              AS stored_amount,
              w.balance_currency            AS currency,
              COALESCE(agg.ledger_delta, 0::NUMERIC(19,2))::NUMERIC(19,2) AS calculated_amount,
              (w.balance_amount - COALESCE(agg.ledger_delta, 0::NUMERIC(19,2)))::NUMERIC(19,2) AS difference,
              COALESCE(agg.checked_entries, 0)::int AS checked_entries
         FROM wallet w
              LEFT JOIN LATERAL (
                SELECT SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END) AS ledger_delta,
                       COUNT(*)::int AS checked_entries
                  FROM wallet_ledger_entry
                 WHERE wallet_id = w.id
              ) agg ON TRUE
        WHERE w.id = ?`,
      [walletId],
    );
    const row = rows[0];
    if (row === undefined) {
      // Wallet inexistente: a própria query devolve zero linhas → 404 na borda.
      return undefined;
    }
    return {
      storedBalance: Money.from({
        amount: row.stored_amount as string,
        currency: row.currency as string,
      }),
      calculatedAmount: row.calculated_amount as string,
      difference: row.difference as string,
      checkedEntries: row.checked_entries as number,
    };
  }
}

function toEntryRow(row: LedgerRowShape): LedgerEntryRow {
  // `created_at` bruto vem como texto pg ("...+00"): derivar o Date do ISO
  // canônico (µs) evita `.toISOString` de Date inexistente na borda.
  return {
    id: row.id as string,
    transactionId: row.transaction_id as string,
    direction: row.direction === 'DEBIT' ? LedgerDirection.Debit : LedgerDirection.Credit,
    money: Money.from({ amount: row.amount as string, currency: row.currency as string }),
    balanceBefore: Money.from({
      amount: row.balance_before_amount as string,
      currency: row.balance_before_currency as string,
    }),
    balanceAfter: Money.from({
      amount: row.balance_after_amount as string,
      currency: row.balance_after_currency as string,
    }),
    createdAt: new Date(row.created_at_iso as string),
  };
}
