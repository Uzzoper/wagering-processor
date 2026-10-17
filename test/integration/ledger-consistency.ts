import { expect } from 'bun:test';

/**
 * Invariante final da spec: `wallet.balance == soma do ledger` e a cadeia
 * termina no saldo atual (existe lançamento com `balance_after == balance`;
 * a busca por recentes só cataloga a linha). Ordens por `ctid` não são
 * confiáveis com inserts concorrentes — por isso a prova por existência.
 * Uso compartilhado pelos testes de integração; cada arquivo passa seu
 * `execute` local. Chamar ao fim de todo teste que mexe em saldo.
 */
export async function assertLedgerConsistency(
  execute: (
    sql: string,
    params?: unknown[],
  ) => Promise<
    Array<{ balance_amount: string; ledger_sum: string | null; last_after: string | null }>
  >,
  walletId: string,
): Promise<void> {
  const rows = await execute(
    `SELECT w.balance_amount,
            SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount ELSE -l.amount END) AS ledger_sum,
            (SELECT balance_after_amount FROM wallet_ledger_entry
              WHERE wallet_id = w.id AND balance_after_amount = w.balance_amount
              ORDER BY created_at DESC, ctid DESC LIMIT 1) AS last_after
       FROM wallet w
       LEFT JOIN wallet_ledger_entry l ON l.wallet_id = w.id
      WHERE w.id = ?
      GROUP BY w.id, w.balance_amount`,
    [walletId],
  );
  expect(rows).toHaveLength(1);
  expect(Number(rows[0]?.balance_amount)).toBe(Number(rows[0]?.ledger_sum ?? '0.00'));
  // `last_after` é NULL quando nenhuma entrada encerra a cadeia no saldo atual.
  expect(Number(rows[0]?.balance_amount)).toBe(Number(rows[0]?.last_after));
}
