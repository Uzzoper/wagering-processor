import { Migration } from '@mikro-orm/migrations';

/**
 * Schema inicial — as 5 tabelas + TODAS as constraints, índices e triggers do
 * ARCHITECTURE.md §3. Migração reversível: `down()` reverte na ordem inversa.
 *
 * Tabelas: wallet, wager_transaction, wallet_ledger_entry, inbox_message, outbox_message.
 * FKs: wager_transaction.wallet_id → wallet(id); wallet_ledger_entry.wallet_id → wallet(id);
 * wallet_ledger_entry.transaction_id → wager_transaction(id); e a self-FK anulável
 * wager_transaction.reference_transaction_id → wager_transaction(id).
 */
export class Migration20261015000000 extends Migration {
  override async up(): Promise<void> {
    // ---- tabelas (colunas base) ----
    this.addSql(`
      CREATE TABLE wallet (
        id UUID NOT NULL,
        player_id UUID NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_amount NUMERIC(19,2) NOT NULL,
        balance_currency CHAR(3) NOT NULL,
        version INTEGER NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        CONSTRAINT wallet_pkey PRIMARY KEY (id)
      )
    `);

    this.addSql(`
      CREATE TABLE wager_transaction (
        id UUID NOT NULL,
        provider_id VARCHAR(255) NOT NULL,
        external_transaction_id VARCHAR(255) NOT NULL,
        idempotency_key VARCHAR(255) NOT NULL,
        payload_hash VARCHAR(64) NOT NULL,
        wallet_id UUID NOT NULL,
        player_id UUID NOT NULL,
        round_id VARCHAR(255) NOT NULL,
        game_id VARCHAR(255) NOT NULL,
        kind VARCHAR(16) NOT NULL,
        money_amount NUMERIC(19,2) NOT NULL,
        money_currency CHAR(3) NOT NULL,
        reference_external_transaction_id VARCHAR(255),
        reference_transaction_id UUID,
        status VARCHAR(32) NOT NULL,
        failure_code VARCHAR(64),
        created_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        CONSTRAINT wager_transaction_pkey PRIMARY KEY (id)
      )
    `);

    this.addSql(`
      CREATE TABLE wallet_ledger_entry (
        id UUID NOT NULL,
        wallet_id UUID NOT NULL,
        transaction_id UUID NOT NULL,
        direction VARCHAR(6) NOT NULL,
        amount NUMERIC(19,2) NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_before_amount NUMERIC(19,2) NOT NULL,
        balance_before_currency CHAR(3) NOT NULL,
        balance_after_amount NUMERIC(19,2) NOT NULL,
        balance_after_currency CHAR(3) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        CONSTRAINT wallet_ledger_entry_pkey PRIMARY KEY (id)
      )
    `);

    this.addSql(`
      CREATE TABLE inbox_message (
        id UUID NOT NULL,
        consumer_name VARCHAR(255) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        payload_hash VARCHAR(64) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        CONSTRAINT inbox_message_pkey PRIMARY KEY (id)
      )
    `);

    this.addSql(`
      CREATE TABLE outbox_message (
        id UUID NOT NULL,
        event_id UUID NOT NULL,
        aggregate_id UUID NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ,
        published_at TIMESTAMPTZ,
        CONSTRAINT outbox_message_pkey PRIMARY KEY (id)
      )
    `);

    // ---- integridade referencial (§3 + fatia 4) ----
    this.addSql(`
      ALTER TABLE wager_transaction ADD CONSTRAINT fk_wager_wallet
      FOREIGN KEY (wallet_id) REFERENCES wallet (id)
    `);
    this.addSql(`
      ALTER TABLE wallet_ledger_entry ADD CONSTRAINT fk_ledger_wallet
      FOREIGN KEY (wallet_id) REFERENCES wallet (id)
    `);
    this.addSql(`
      ALTER TABLE wallet_ledger_entry ADD CONSTRAINT fk_ledger_transaction
      FOREIGN KEY (transaction_id) REFERENCES wager_transaction (id)
    `);
    this.addSql(`
      ALTER TABLE wager_transaction ADD CONSTRAINT fk_wager_reference
      FOREIGN KEY (reference_transaction_id) REFERENCES wager_transaction (id)
    `);

    // ---- unicidade / identidade (§3) ----
    this.addSql(`ALTER TABLE wallet ADD CONSTRAINT uq_wallet_player_currency UNIQUE (player_id, currency)`);
    this.addSql(`ALTER TABLE wager_transaction ADD CONSTRAINT uq_wager_idempotency UNIQUE (idempotency_key)`);
    this.addSql(
      `ALTER TABLE wager_transaction ADD CONSTRAINT uq_wager_provider_external UNIQUE (provider_id, external_transaction_id)`,
    );
    this.addSql(
      `ALTER TABLE inbox_message ADD CONSTRAINT uq_inbox_consumer_msg UNIQUE (consumer_name, message_id)`,
    );
    this.addSql(
      `ALTER TABLE wallet_ledger_entry ADD CONSTRAINT uq_ledger_tx_wallet UNIQUE (transaction_id, wallet_id)`,
    );
    this.addSql(`ALTER TABLE outbox_message ADD CONSTRAINT uq_outbox_event UNIQUE (event_id)`);

    // ---- saldo observado no processamento (§3) ----
    this.addSql(`ALTER TABLE wager_transaction ADD COLUMN observed_balance_amount NUMERIC(19,2)`);
    this.addSql(`ALTER TABLE wager_transaction ADD COLUMN observed_balance_currency CHAR(3)`);

    // ---- reversão única por referência, qualquer tipo (§3) ----
    this.addSql(`
      CREATE UNIQUE INDEX uq_wager_single_reversal ON wager_transaction (reference_transaction_id)
      WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED'
    `);

    // ---- não-negatividade no banco (§3) ----
    this.addSql(`ALTER TABLE wallet ADD CONSTRAINT ck_wallet_balance_nonneg CHECK (balance_amount >= 0)`);
    this.addSql(`ALTER TABLE wallet ADD CONSTRAINT ck_wallet_version_pos CHECK (version >= 1)`);
    // a moeda do saldo nunca diverge da moeda da wallet (§3)
    this.addSql(
      `ALTER TABLE wallet ADD CONSTRAINT ck_wallet_balance_currency CHECK (balance_currency = currency)`,
    );

    // ---- ledger balanceado via CHECK (§3) ----
    this.addSql(`ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_amount_pos CHECK (amount > 0)`);
    this.addSql(`
      ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_balanced CHECK (
        (direction = 'DEBIT' AND balance_after_amount = balance_before_amount - amount)
        OR (direction = 'CREDIT' AND balance_after_amount = balance_before_amount + amount)
      )
    `);
    this.addSql(`
      ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_same_currency CHECK (
        balance_before_currency = balance_after_currency
        AND balance_before_currency = currency
      )
    `);

    // ---- imutabilidade do ledger (§3) ----
    this.addSql(`
      CREATE FUNCTION trg_ledger_immutable() RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entry is immutable (attempted %)', TG_OP
          USING ERRCODE = 'restrict_violation';
      END;
      $$ LANGUAGE plpgsql
    `);
    this.addSql(`
      CREATE TRIGGER trg_ledger_immutable
      BEFORE UPDATE OR DELETE ON wallet_ledger_entry
      FOR EACH ROW EXECUTE FUNCTION trg_ledger_immutable()
    `);

    // ---- imutabilidade de linha terminal (§3, descrito no texto) ----
    this.addSql(`
      CREATE FUNCTION trg_wager_terminal_immutable() RETURNS trigger AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
          RAISE EXCEPTION 'wager_transaction % is terminal (%) and cannot be updated', OLD.id, OLD.status
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);
    this.addSql(`
      CREATE TRIGGER trg_wager_terminal_immutable
      BEFORE UPDATE ON wager_transaction
      FOR EACH ROW EXECUTE FUNCTION trg_wager_terminal_immutable()
    `);

    // ---- índices de disputa e varredura (§3) ----
    this.addSql(`CREATE INDEX ix_wager_wallet_status ON wager_transaction (wallet_id, status)`);
    this.addSql(
      `CREATE INDEX ix_wager_reference ON wager_transaction (provider_id, reference_external_transaction_id)`,
    );
    this.addSql(`CREATE INDEX ix_ledger_wallet_id ON wallet_ledger_entry (wallet_id, id)`);

    // ---- outbox: next_attempt_at nunca NULL (§3) ----
    this.addSql(`ALTER TABLE outbox_message ALTER COLUMN next_attempt_at SET NOT NULL`);
    this.addSql(`ALTER TABLE outbox_message ALTER COLUMN next_attempt_at SET DEFAULT now()`);
    this.addSql(
      `CREATE INDEX ix_outbox_pending ON outbox_message (next_attempt_at) WHERE published_at IS NULL`,
    );

    // ---- PENDING_REFERENCE: tentativas + marco inicial do TTL (§3) ----
    this.addSql(`ALTER TABLE wager_transaction ADD COLUMN retry_attempts INTEGER NOT NULL DEFAULT 0`);
    this.addSql(`ALTER TABLE wager_transaction ADD COLUMN pending_since TIMESTAMPTZ`);
    this.addSql(`ALTER TABLE wager_transaction ADD COLUMN next_retry_at TIMESTAMPTZ`);
    this.addSql(`
      CREATE INDEX ix_wager_pending_ref ON wager_transaction (status, next_retry_at)
      WHERE status = 'PENDING_REFERENCE'
    `);
  }

  override async down(): Promise<void> {
    // Ordem inversa do `up`.
    this.addSql(`DROP INDEX IF EXISTS ix_wager_pending_ref`);
    this.addSql(`ALTER TABLE wager_transaction DROP COLUMN IF EXISTS next_retry_at`);
    this.addSql(`ALTER TABLE wager_transaction DROP COLUMN IF EXISTS pending_since`);
    this.addSql(`ALTER TABLE wager_transaction DROP COLUMN IF EXISTS retry_attempts`);

    this.addSql(`DROP INDEX IF EXISTS ix_outbox_pending`);
    this.addSql(`ALTER TABLE outbox_message ALTER COLUMN next_attempt_at DROP DEFAULT`);
    this.addSql(`ALTER TABLE outbox_message ALTER COLUMN next_attempt_at DROP NOT NULL`);

    this.addSql(`DROP INDEX IF EXISTS ix_ledger_wallet_id`);
    this.addSql(`DROP INDEX IF EXISTS ix_wager_reference`);
    this.addSql(`DROP INDEX IF EXISTS ix_wager_wallet_status`);

    this.addSql(`DROP TRIGGER IF EXISTS trg_wager_terminal_immutable ON wager_transaction`);
    this.addSql(`DROP FUNCTION IF EXISTS trg_wager_terminal_immutable()`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_ledger_immutable ON wallet_ledger_entry`);
    this.addSql(`DROP FUNCTION IF EXISTS trg_ledger_immutable()`);

    this.addSql(`DROP INDEX IF EXISTS uq_wager_single_reversal`);
    this.addSql(`ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS ck_ledger_same_currency`);
    this.addSql(`ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS ck_ledger_balanced`);
    this.addSql(`ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS ck_ledger_amount_pos`);
    this.addSql(`ALTER TABLE wallet DROP CONSTRAINT IF EXISTS ck_wallet_balance_currency`);
    this.addSql(`ALTER TABLE wallet DROP CONSTRAINT IF EXISTS ck_wallet_version_pos`);
    this.addSql(`ALTER TABLE wallet DROP CONSTRAINT IF EXISTS ck_wallet_balance_nonneg`);

    // FKs explícitas antes de dropar as tabelas (reversão limpa e independente).
    this.addSql(`ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS fk_ledger_transaction`);
    this.addSql(`ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS fk_ledger_wallet`);
    this.addSql(`ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS fk_wager_reference`);
    this.addSql(`ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS fk_wager_wallet`);

    this.addSql(`DROP TABLE IF EXISTS outbox_message`);
    this.addSql(`DROP TABLE IF EXISTS inbox_message`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entry`);
    this.addSql(`DROP TABLE IF EXISTS wager_transaction`);
    this.addSql(`DROP TABLE IF EXISTS wallet`);
  }
}
