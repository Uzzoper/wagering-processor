import { expect } from 'bun:test';

/**
 * Invariante final da spec: `wallet.balance == saldo reconstruído pelo ledger`
 * (e igual ao `balance_after` do último lançamento, desempate por `ctid`).
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
              WHERE wallet_id = w.id ORDER BY created_at DESC, ctid DESC LIMIT 1) AS last_after
       FROM wallet w
       LEFT JOIN wallet_ledger_entry l ON l.wallet_id = w.id
      WHERE w.id = ?
      GROUP BY w.id, w.balance_amount`,
    [walletId],
  );
  expect(rows).toHaveLength(1);
  expect(Number(rows[0]?.balance_amount)).toBe(Number(rows[0]?.ledger_sum ?? '0.00'));
  if (rows[0]?.last_after !== null && rows[0]?.last_after !== undefined) {
    expect(Number(rows[0]?.balance_amount)).toBe(Number(rows[0]?.last_after));
  }
}
