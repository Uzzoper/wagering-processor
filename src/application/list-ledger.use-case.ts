import type { LedgerEntryRow, WalletReadStore } from './ports';
import { encodeLedgerCursor, type LedgerCursor } from './ledger-cursor';

/**
 * Página do ledger do wallet em ordem total e estável `(created_at, id) ASC`
 * via keyset (nunca OFFSET). `nextCursor` presente só quando a página encheu
 * (página cheia seguida de vazia é aceitável no padrão). `undefined` =
 * wallet inexistente → borda responde `404 WALLET_NOT_FOUND`.
 */
export class ListLedgerUseCase {
  constructor(private readonly readStore: WalletReadStore) {}

  async execute(
    walletId: string,
    query: { cursor: LedgerCursor | undefined; limit: number },
  ): Promise<{ items: LedgerEntryRow[]; nextCursor: string | null } | undefined> {
    const page = await this.readStore.ledgerPage(walletId, query.cursor, query.limit);
    if (page === undefined) {
      return undefined;
    }
    const last = page.items[page.items.length - 1];
    return {
      items: page.items,
      // O store sinaliza próxima página pelo look-ahead: cursor é o par exato
      // da última linha devolvida — keyset reinjeia sem pular nem repetir.
      nextCursor:
        last !== undefined && page.cursorCreatedAt !== undefined
          ? encodeLedgerCursor({ createdAt: page.cursorCreatedAt, id: last.id })
          : null,
    };
  }
}
