import { Migration } from '@mikro-orm/migrations';

/**
 * Fatia 8a: keyset pagination do ledger precisa de índice por `(wallet_id,
 * created_at, id)` — a ordem de leitura é `(created_at, id) ASC` (o `id` é
 * UUID aleatório, não ordenável; a ordem fica no par). Reversível: o `down`
 * apenas remove o índice.
 */
export class Migration20261017000000 extends Migration {
  override async up(): Promise<void> {
    this.addSql(
      'CREATE INDEX idx_ledger_wallet_created_id ON wallet_ledger_entry (wallet_id, created_at, id)',
    );
  }

  override async down(): Promise<void> {
    this.addSql('DROP INDEX IF EXISTS idx_ledger_wallet_created_id');
  }
}
