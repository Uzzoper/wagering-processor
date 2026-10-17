# ARCHITECTURE.md — Wagering Processor

> Status: implementado (fatias 1 a 8c completas). Spec fiel em `CHALLENGE.md` (não editar).

## 1. Visão geral e boundaries

Sistema financeiro distribuído que recebe operações `BET → WIN | LOSS | REFUND | ROLLBACK` por HTTP e por SQS, com correção garantida pelo banco, não pelo broker.

```text
                +------------------+
                |       api/       |  controllers Nest, DTOs, guards, filtros HTTP
                +--------+---------+
                         | chama
                +--------v---------+
                |   application/   |  use-cases + portas (mesmo código para HTTP e SQS)
                +--------+---------+
                         | usa
          +--------------+--------------+
          |              |              |
   domain/ (puro)   infra/ (efeitos)  workers
```

**Camadas e regras de dependência:**

| Camada | Papel | Pode importar | Não pode importar |
|---|---|---|---|
| `src/domain/` | `Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `InboxMessage`, `OutboxMessage`, `IntegrationEvent` + erros. Puro, sem Nest/ORM. Testável com `bun test` sem infra | `decimal.js` | `@mikro-orm/*`, `@nestjs/*` |
| `src/application/` | Use-cases: `SubmitWagerTransaction`, `CreateWallet`, `ReconcileWallet`, `ReprocessPendingReference`. Portas: `WalletRepository`, `TransactionStore`, `LedgerStore`, `InboxStore`, `OutboxStore`, `InboxMessage`/`OutboxMessage` no domínio, `Clock`, `IdGenerator`, `ProviderIdentityPort`, `EventPublisherPort`. Validador de entrada compartilhado (`wager-input.ts`): HTTP e SQS usam exatamente o mesmo parser (Boundary invertida: a borda HTTP só traduz `WagerInputError` → `BusinessError`) | `domain` | `infra`, `api` |
| `src/infra/` | Entities MikroORM, migrations, repositórios, SQS consumer/publisher, workers `outbox` e `pending-reference`, logs/métricas, health checks | `domain`, `application` | — |
| `src/api/` | Controllers, DTOs (`MoneyProps` como string), validação de entrada, mapeamento `failureCode → HTTP`, `Idempotency-Key` | `application`, `domain` (só tipos) | SQL direto |

**Use-case compartilhado HTTP+SQS (decisão travada):**

* Existe um único `SubmitWagerTransactionUseCase.execute(input)` que recebe um input normalizado (sem saber se veio de HTTP ou SQS).
* `POST /wagering/transactions` apenas valida DTO + extrai `Idempotency-Key` do header e chama o use-case dentro de `em.transactional()`.
* O consumer SQS monta o mesmo input a partir de `data` da mensagem + `messageId`, abre a mesma transação SQL (incluindo `inbox`), chama o mesmo use-case, faz commit, e só então faz `ack` (delete).
* A entrada SQS persiste o dedup da inbox dentro da MESMA tx pelo gancho `execute(input, { beforeRepositories })` — passo 1 da ordem do §2; a borda HTTP não usa o gancho.
* Diferença fica na borda: HTTP retorna resposta síncrona (`PROCESSED`/`REJECTED`/`PENDING_REFERENCE`/conflito); SQS classifica o erro em terminal (ack) / transitório (libera visibilidade) / permanente (DLQ). A regra de negócio nunca se bifurca.

Criação de wallet (`POST /wallets`) segue o mesmo padrão: `CreateWalletUseCase` cria `Wallet` + `WagerTransaction(OPENING)` + 1 `ledger CREDIT` (se saldo > 0) + `WalletBalanceChanged` + `WagerTransactionProcessed`, tudo na mesma tx.

## 2. Decisão ORM: MikroORM

**Decisão: MikroORM 7 (preferencial do desafio). Não usar Prisma nem TypeORM.**

| Opção | Veredito | Motivo |
|---|---|---|
| MikroORM | **escolhido** | `EntityManager.transactional()` explícito, `LockMode.PESSIMISTIC_WRITE` nativo, Identity Map + Unit of Work por request (`em.fork()`), migrations versionadas/reversíveis. Casa com "mesma tx SQL para inbox+saldo+ledger+outbox" sem gambiarra |
| TypeORM | rejeitado | Aceito pelo desafio, mas `QueryRunner`/transação e locking são mais implícitos e a API de `pessimistic_write` + `skip_locked` para outbox é mais frágil |
| Prisma | proibido | Fora de escopo pelo desafio. Esconde Unit of Work, não expõe `FOR UPDATE` / `SKIP LOCKED` de forma natural, dificulta lock por `walletId` e migrations com checks customizados |

**Estratégia transacional (travada):**

* Todo request/worker faz `em.fork()` (um `EntityManager` por execução) e roda `em.transactional(async em => ...)` com `READ COMMITTED`.
* Dentro da tx, ordem fixa: 1) `inbox` (só SQS), 2) insert idempotente da transação (`ON CONFLICT DO NOTHING`, ver §4), 3) `SELECT wallet ... FOR UPDATE`, 4) aplica regra de negócio em objetos de domínio puros, 5) persiste wallet + transação + ledger (se houver) + outbox. Commit único.
* No Submit (5b): antes do insert, `SELECT` de existência da wallet — não achou → `404 WALLET_NOT_FOUND` sem persistir nada (a FK abortaria a tx com `23503`). Depois insert idempotente, depois `FOR UPDATE`.
* Entidades MikroORM são "espelhos" finos: só colunas + índices. Regra vive no domínio; entity converte via `rehydrate` (sem revalidar) na leitura e via `toPersistence()` na escrita.
* Nunca publicar evento, nunca fazer `ack` SQS, nunca responder HTTP antes do commit.
* Rejeição de negócio nunca lança exceção: o use-case RETORNA `PROCESSED`, `REJECTED` ou `PENDING_REFERENCE`, a tx faz commit (persistindo a linha `REJECTED` + evento `WagerTransactionRejected`) e só a borda mapeia para HTTP `422`. Apenas erro transitório de infraestrutura lança exceção e causa rollback.
* `SET LOCAL lock_timeout = '3s'` no início de toda tx financeira. Erros `55P03` (lock timeout) e `40P01` (deadlock) viram `503 INFRA_TRANSIENT` com retry na borda — nunca `REJECTED` de negócio.

**Mapeamento de `Money` (travado): duas colunas separadas, representação exata.**

* `amount NUMERIC(19,2) NOT NULL` + `currency CHAR(3) NOT NULL`. Exemplo: `balance_amount`, `balance_currency`.
* Por que `NUMERIC(19,2)` e não `float`/`double`, `integer` de centavos ou `varchar`: é exato em PG, impõe escala 2 no schema, permite `CHECK (balance_amount >= 0)` e ordenação/soma no banco para reconciliação. `decimal.js` no domínio; driver devolve string, convertemos sem passar por `number`.
* `Money` do domínio nunca importa tipo do ORM nem decorator do Nest. DTOs e eventos usam `MoneyProps { amount: string; currency: string }` com regex `^\d+\.\d{2}$` na borda (rejeita `NaN`, `Infinity`, `1e3`, `""`, `25.1`, `25.123`, negativo em input). Moeda em ISO-4217 (`^[A-Z]{3}$`) no domínio e na borda.
* Escopo de moeda: código é multi-moeda (toda operação checa `assertSameCurrency`, cross-currency é erro de domínio), mas testes e seed assumem `BRL`. Conflito de moeda é testado e mapeado como `422`.

## 3. Concorrência por `walletId`

**Decisão: pessimistic locking (`SELECT ... FOR UPDATE` na linha da wallet) como mecanismo primário + constraints no banco como garantia final. Não usar optimistic+retry como caminho principal nem `UPDATE` atômico condicional isolado.**

Fundamentação para o cenário `100.00 BRL` + 2× `BET 80.00` em paralelo (e 50× replay, ≥3 instâncias):

* O problema é `check-then-act` (ler saldo, decidir, debitar) com ledger + outbox atômicos. `UPDATE wallets SET balance = balance - 80 WHERE balance >= 80` sozinho resolve o débito simples, mas não cobre `REFUND`/`ROLLBACK` (precisa validar referência + igualdade de valor + "uma única vez" + vínculo provider/player/wallet/moeda/round), nem a escrita atômica de transação + ledger + outbox + inbox.
* Optimistic locking (`version` + retry) funciona, mas sob hot wallet (muitas apostas na mesma wallet) gera avalanche de retries, aumenta latência p95/p99 e complica o teste `100/80x2` (qual retry vence é não-determinístico e exige janela de retry bem calibrada). Mantemos `version` como coluna de auditoria/detecção (incrementa só quando saldo muda, começa em `1`), não como controle principal.
* `FOR UPDATE` por `walletId` serializa só quem disputa a mesma wallet; wallets distintas seguem em paralelo total; 3+ instâncias continuam corretas porque a serialização está no PG, não em memória. É simples de raciocinar e de testar. Custo assumido: throughput por wallet vira sequencial (ver §9).

**Regras travadas:**

* Unidade de concorrência = `walletId`. Nenhum lock global, nenhum mutex em memória.
* Toda tx financeira que mexe em saldo executa `SELECT ... FOR UPDATE` cru na linha da wallet (`src/infra/persistence/wallet.repository.ts`) antes de decidir. `LOSS` (sem efeito em saldo) também pega o lock se precisar ordenar com `BET` da mesma rodada — barato e evita surpresa; `REJECTED` fora de saldo insuficiente não precisa de lock, mas o caminho de código é o mesmo para não bifurcar.
* Ordem de locks fixa (`walletId` asc) caso um dia uma tx toque 2 wallets (hoje não há; travado: max 1 ledger por wallet por tx). Conflito de lock é métrica (`db_lock_conflicts_total`) e vira erro transitório com retry na borda (HTTP `503` / SQS sem ack), nunca `REJECTED` de negócio.
* O lock da wallet serializa concorrentes da mesma wallet, então checagens como "referência já revertida?" enxergam a linha commitada do vencedor — o índice único de reversão abaixo é a guarda final, não o caminho feliz.
* Nunca `read → calculate → update` sem lock (falha eliminatória).

**Schema: constraints e índices (parte da avaliação — vão para migrations reversíveis):**

```sql
-- unicidade / identidade
ALTER TABLE wallet ADD CONSTRAINT uq_wallet_player_currency UNIQUE (player_id, currency);
ALTER TABLE wager_transaction ADD CONSTRAINT uq_wager_idempotency UNIQUE (idempotency_key);
ALTER TABLE wager_transaction ADD CONSTRAINT uq_wager_provider_external UNIQUE (provider_id, external_transaction_id);
ALTER TABLE inbox_message ADD CONSTRAINT uq_inbox_consumer_msg UNIQUE (consumer_name, message_id);
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT uq_ledger_tx_wallet UNIQUE (transaction_id, wallet_id);
ALTER TABLE outbox_message ADD CONSTRAINT uq_outbox_event UNIQUE (event_id);

-- saldo observado no processamento (para replay exato, inclusive LOSS/REJECTED sem ledger — ver §4)
ALTER TABLE wager_transaction ADD COLUMN observed_balance_amount NUMERIC(19,2);
ALTER TABLE wager_transaction ADD COLUMN observed_balance_currency CHAR(3);

-- reversão única por referência, qualquer tipo (ver interpretação adicional abaixo)
CREATE UNIQUE INDEX uq_wager_single_reversal ON wager_transaction (reference_transaction_id)
  WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- não-negatividade no banco, não só em código
ALTER TABLE wallet ADD CONSTRAINT ck_wallet_balance_nonneg CHECK (balance_amount >= 0);
ALTER TABLE wallet ADD CONSTRAINT ck_wallet_version_pos CHECK (version >= 1);
-- a moeda do saldo nunca diverge da moeda da wallet
ALTER TABLE wallet ADD CONSTRAINT ck_wallet_balance_currency CHECK (balance_currency = currency);

-- ledger: direção válida e saldo final não-negativo
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT'));
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_after_nonneg CHECK (balance_after_amount >= 0);

-- ledger balanceado via CHECK (simples, roda em toda escrita — sem trigger de aritmética)
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_amount_pos CHECK (amount > 0);
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_balanced CHECK (
  (direction = 'DEBIT' AND balance_after_amount = balance_before_amount - amount)
  OR (direction = 'CREDIT' AND balance_after_amount = balance_before_amount + amount)
);
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT ck_ledger_same_currency CHECK (
  balance_before_currency = balance_after_currency
  AND balance_before_currency = currency
);

-- domínio de wager_transaction: valores válidos
ALTER TABLE wager_transaction ADD CONSTRAINT ck_wager_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'));
ALTER TABLE wager_transaction ADD CONSTRAINT ck_wager_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'));
ALTER TABLE wager_transaction ADD CONSTRAINT ck_wager_money_pos CHECK (money_amount >= 0 AND (kind = 'LOSS' OR money_amount > 0));
ALTER TABLE wager_transaction ADD CONSTRAINT ck_wager_ref_required CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL);
ALTER TABLE wager_transaction ADD CONSTRAINT ck_wager_failure_code CHECK (status NOT IN ('REJECTED', 'FAILED') OR failure_code IS NOT NULL);

-- imutabilidade do ledger: trigger trg_ledger_immutable (RAISE EXCEPTION em UPDATE/DELETE).
-- TRUNCATE não dispara trigger FOR EACH ROW: o trigger trg_ledger_no_truncate (FOR EACH STATEMENT) reusa a mesma função.
-- REVOKE UPDATE/DELETE só ajuda se a app usar role diferente do dono das tabelas; o trigger é a garantia real.

-- índices de disputa e varredura
CREATE INDEX ix_wager_wallet_status ON wager_transaction (wallet_id, status);
CREATE INDEX ix_wager_reference ON wager_transaction (provider_id, reference_external_transaction_id);
CREATE INDEX ix_ledger_wallet_id ON wallet_ledger_entry (wallet_id, id); -- paginação por cursor
-- outbox: `next_attempt_at` nunca NULL e já nasce com default (o claim
-- `next_attempt_at <= now()` não pega linhas NULL)
CREATE TABLE outbox_message (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX ix_outbox_pending ON outbox_message (next_attempt_at) WHERE published_at IS NULL;
-- PENDING_REFERENCE: tentativas + marco inicial para calcular o TTL de 24h
ALTER TABLE wager_transaction ADD COLUMN retry_attempts INT NOT NULL DEFAULT 0;
ALTER TABLE wager_transaction ADD COLUMN pending_since TIMESTAMPTZ;
ALTER TABLE wager_transaction ADD COLUMN next_retry_at TIMESTAMPTZ;
CREATE INDEX ix_wager_pending_ref ON wager_transaction (status, next_retry_at) WHERE status = 'PENDING_REFERENCE';

-- integridade referencial (fatia 4; exige criar as tabelas-pai antes)
ALTER TABLE wager_transaction ADD CONSTRAINT fk_wager_wallet
  FOREIGN KEY (wallet_id) REFERENCES wallet (id);
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT fk_ledger_wallet
  FOREIGN KEY (wallet_id) REFERENCES wallet (id);
ALTER TABLE wallet_ledger_entry ADD CONSTRAINT fk_ledger_transaction
  FOREIGN KEY (transaction_id) REFERENCES wager_transaction (id);
-- self-FK anulável: uma reversão pode apontar para a transação original
ALTER TABLE wager_transaction ADD CONSTRAINT fk_wager_reference
  FOREIGN KEY (reference_transaction_id) REFERENCES wager_transaction (id);
```

Reversão (`down`) das FKs e dos CHECKs, antes de dropar as tabelas:

```sql
ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS fk_ledger_transaction;
ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS fk_ledger_wallet;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS fk_wager_reference;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS fk_wager_wallet;
DROP TRIGGER IF EXISTS trg_ledger_no_truncate ON wallet_ledger_entry;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS ck_wager_failure_code;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS ck_wager_ref_required;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS ck_wager_money_pos;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS ck_wager_status;
ALTER TABLE wager_transaction DROP CONSTRAINT IF EXISTS ck_wager_kind;
ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS ck_ledger_after_nonneg;
ALTER TABLE wallet_ledger_entry DROP CONSTRAINT IF EXISTS ck_ledger_direction;
```

* `PROCESSED/REJECTED/FAILED` imutáveis: garantido por código (transição lança `InvalidTransactionStateError` se terminal) + trigger que bloqueia `UPDATE` que mude linha terminal + ledger sem `UPDATE/DELETE`.
* `OPENING` interno: validação só na borda API/SQS, que rejeita `kind=OPENING` com `400` (sem `CHECK` de `created_by` no schema).
* Interpretação adicional (reversão única): a spec diz "não pode ser revertida duas vezes pelo mesmo tipo", mas permitir `REFUND` + `ROLLBACK` sobre a mesma `BET` creditaria duas vezes. Travado: **uma única reversão por referência, qualquer tipo**, garantida no banco pelo índice parcial acima. Mais conservador e auditável; registrado aqui como interpretação adicional conforme a spec permite.
* Interpretação adicional (referência do WIN): a spec permite que o WIN referencie a BET, sem dizer o que fazer quando a referência falta ou é inválida. Travado: referência do WIN é opcional e informativa — vincula quando resolve para transação `PROCESSED`, senão segue sem vínculo; WIN nunca vai para `PENDING_REFERENCE`.

## 4. Idempotência

**Decisão: `Idempotency-Key` (header no HTTP, campo `idempotencyKey` no SQS) é a fonte da verdade. `payloadHash` detecta conflito.**

**Algoritmo `payloadHash` (travado, documentado para o provedor):**

1. Monta objeto de negócio com exatamente estes campos, nesta semântica: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money.amount` (normalizado para 2 casas, ex. `"25.00"`), `money.currency`, `referenceExternalTransactionId` (ou `null` se ausente).
2. Header `Idempotency-Key` e metadados de transporte (`messageId`, timestamps, `correlationId`) **não** entram no hash.
3. Canonicalização: ordena chaves recursivamente, UTF-8, sem espaços, separadores `,` e `:` padrão `JSON.stringify` canônico.
4. `SHA-256` hex minúsculo do JSON canônico.

O hash canônico vive em `application` como função pura com teste próprio: mesmo payload com ordem de campos diferente gera o mesmo hash (a borda já rejeita `25.0`, então a normalização recebe sempre 2 casas).

**Fluxo (dentro da mesma tx SQL):**

* Tentativa de `INSERT ... ON CONFLICT DO NOTHING` (sem alvo — cobre qualquer violação de unicidade, `idempotency_key` ou `(provider_id, external_transaction_id)`) `RETURNING ...`. Se retornou linha → somos o vencedor, segue o processamento. Se não retornou → `SELECT` por `idempotency_key` ou por `(provider_id, external_transaction_id)`: o perdedor bloqueia até o commit do vencedor e depois enxerga a linha (replay ou conflito abaixo). Motivo: no Postgres um erro de constraint (`23505`) aborta a transação inteira — `ON CONFLICT (idempotency_key)` deixaria a outra unique descoberta e o `23505` dela abortaria a tx do mesmo jeito.
* Se existe e `payloadHash` igual → **replay**: retorna exatamente a resposta original (`transactionId`, `status`, `observedBalance` guardado, `idempotentReplay: true`). Não reexecuta regra, não cria ledger, não publica novo evento.
* Se existe e `payloadHash` diferente → **conflito**, não replay: HTTP `409` com `failureCode: IDEMPOTENCY_CONFLICT`; SQS trata como terminal (ack, sem reexecução) e publica nada novo. Permanece auditável.
* Dupla unicidade (`idempotency_key` e `(provider_id, external_transaction_id)`): mesma `externalTransactionId` com `Idempotency-Key` diferente → `409 IDEMPOTENCY_CONFLICT`. As duas chaves identificam a mesma operação; divergir é conflito, não operação nova.
* Colunas `observed_balance_amount/currency` em `wager_transaction`, preenchidas no processamento (inclusive `LOSS`/`REJECTED`/`PENDING_REFERENCE`, que não têm ledger) — replay devolve o saldo observado naquele momento, conforme a regra 7 da spec.

Resposta HTTP carrega `idempotentReplay: boolean` sempre. `GET /providers/:providerId/wagering/transactions/:externalTransactionId` e `GET /wagering/transactions/:transactionId` retornam a mesma visão (status + balance da época + `failureCode`).

## 5. Inbox / Outbox atômica, SQS, retries, `PENDING_REFERENCE`

**Decisão atômica (travada): inbox + transação + saldo + ledger + outbox na mesma tx SQL. `ack` SQS só pós-commit. Nunca publicar antes do commit (falha eliminatória).**

Filas: `wager-transactions.fifo` + `wager-transactions-dlq.fifo` (LocalStack) e `wager-events.fifo` (eventos da outbox). `MessageGroupId = walletId` (otimização de ordenação, não garantia), `MessageDeduplicationId = messageId`.

**Consumer SQS:**

* Deduplica por inbox persistente `(consumerName='wager-sqs-consumer', messageId)` com `INSERT ... ON CONFLICT (consumer_name, message_id) DO NOTHING RETURNING` na mesma tx — pelo mesmo motivo do §4 (`23505` aborta a transação). Se não retornou linha → redelivery: retorna o efeito original sem tocar em saldo.
* O consumer monta o input pelo mesmo validador de DTO da borda HTTP — a regra "zero só em LOSS" (e kind/`MoneyProps`/referência obrigatória) vale para as duas entradas.
* Classificação de erro (travada): negócio (`REJECTED`, conflito idempotente, `LOSS` ok) → terminal, **ack**; transitório (PG/SQS fora, lock timeout `55P03`, deadlock `40P01`, `503`) → **não** ack, libera visibilidade com backoff, conta `sqs_retries_total`; permanente (payload inválido após N tentativas, `FAILED`) → DLQ após limite.
* Limite SQS: `maxReceiveCount = 5`, `visibilityTimeout = 30s` com heartbeat via `ChangeVisibility` enquanto a tx roda. Em `SIGTERM`: para de puxar, conclui o em andamento ou estende visibilidade para outro assumir (nunca ack sem commit).
* Cenário "morreu depois do commit e antes do ack": outra instância recebe de novo, acha inbox + transação terminal, responde replay e faz ack. Sem duplo débito por construção.

**Worker outbox (múltiplos publishers concorrentes):**

* Polling a cada `500ms`, lote `50`. Claim com lease em tx curta: `UPDATE outbox_message SET next_attempt_at = now() + lease (ex.: 60s) WHERE id IN (SELECT id FROM outbox_message WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY occurred_at LIMIT 50 FOR UPDATE SKIP LOCKED) RETURNING ...` — dois publishers não pegam a mesma linha.
* Publica na `wager-events.fifo` com `MessageGroupId = aggregateId` e `MessageDeduplicationId = eventId`, depois `markPublished` (`published_at = now()`) em tx curta. Se o processo morrer entre o claim e a publicação, a linha volta a ficar vencida ao expirar o lease e outra instância assume — exatamente o cenário obrigatório da seção 11. Falha de publicação → `scheduleRetry` com backoff exponencial `2s → 4s → 8s ... teto 5min`. Duplicata de publicação é segura: envelope tem `eventId` único e consumidor é idempotente por `eventId`.
* Métrica `outbox.lag_seconds` (now − `occurredAt` do mais antigo pendente) com alerta se > 60s.

**Worker `PENDING_REFERENCE` (fora de ordem: `REFUND`/`ROLLBACK` antes da referência):**

* Referência ausente → persiste `PENDING_REFERENCE` + evento `WagerTransactionPendingReference`, sem mexer em saldo.
* Worker agendado a cada `30s` reprocessa pelo mesmo caminho com lock da wallet. Regras de resolução: referência `PROCESSED` → aplica a reversão e preenche `observed_balance_*` com o saldo no momento da aplicação; referência `REJECTED` → rejeita como `INVALID_REFERENCE`; referência ainda `PENDING_REFERENCE` → continua pendente (renova tentativa).
* Limite travado e justificado: **TTL 24h e máx. 20 tentativas, backoff exponencial com jitter (30s → ... → 1h)**, configuráveis por env (`PENDING_REF_TTL_HOURS`, `PENDING_REF_MAX_ATTEMPTS`; poll/lease por `PENDING_REF_POLL_INTERVAL_MS`) para testes usarem TTL curto. 24h cobre atraso operacional de provedor sem virar lixo eterno; 20 tentativas com teto de 1h cabe no TTL sem spam. Esgotou → `REJECTED` com `REFERENCE_NOT_FOUND` + evento `WagerTransactionRejected`. Valor auditável, sem correção silenciosa.
* O worker reusa o MESMO use case de submissão via contexto `pendingResolution`: replay da linha `PENDING_REFERENCE` com essa opção entra em RESUME (lock da wallet + re-leitura anti-corrida) em vez do replay `202` do HTTP. Claim por lease + `SKIP LOCKED` igual à outbox.
* Renovação (referência ainda pendente) incrementa `retry_attempts` + backoff com jitter **sem reemitir** `WagerTransactionPendingReference` — só o PRIMEIRO defer publica o evento; terminal final (PROCESS/REJEIT) zera as colunas de pending.

**Ganchos de crash recovery (exclusivos de teste, fatia 7d):**

* `FAULT_EXIT_AFTER_COMMIT=1` mata o processo em `src/infra/sqs/sqs-wager-consumer.ts` exatamente depois do commit da tx da mensagem e antes do primeiro `DeleteMessage`; `FAULT_EXIT_AFTER_CLAIM=1` mata em `src/infra/workers/outbox-publisher.ts` logo após o claim (lease tomado) e antes da primeira publicação — o publisher aceita `FAULT_EXIT_CLAIM_AGGREGATE=<id>` para disparar só quando o lote claimado contém uma linha desse aggregate (o boot pode claimar linhas alheias da suíte). Ambos logam `test.fault_exit` e só existem em `NODE_ENV=test` (nunca ativam fora dele), mutando o processo real para provar morte entre commit/ack e claim/publicação — a redelivery entra pelo replay via inbox e o lease da outbox expira e é assumido.
* Complemento do `src/main.ts` (descoberto nos testes 7d): em modo **somente worker** (`sqs`/`outbox`/`pending` sem `api`) não há listener HTTP e o event loop do Bun encerra o processo logo após os hooks — o `main.ts` instala um keepalive (`setInterval` enorme) para o worker ficar vivo. Em qualquer modo, `SIGTERM` no processo filho encerra com exit 0 determinístico: a invariante "nada perdido" vem do banco (redelivery/replay via inbox, lease da outbox), não do shutdown gracioso, e os workers são responsáveis por chamar os próprios loops gravidamente via `OnApplicationShutdown` quando aplicável.

## 6. Auth (deferida, ponto de extensão implementado)

**Decisão: deferir auth real. Não vale pontos; não pode competir com correção financeira.**

O ponto de extensão existe no código, sem mudar comportamento:

* `ProviderIdentityPort` em `src/application/ports.ts` (método `resolve(providerId): string`); implementação `NoopProviderIdentity` em `src/infra/auth/noop-provider-identity.ts` aceita o `providerId` do payload e valida apenas o formato (não vazio, tamanho máximo). Identidade inválida é rejeição de negócio: `INVALID_PROVIDER` (`422`), com linha `REJECTED` persistida e evento `WagerTransactionRejected`.
* `AuthGuard` no-op em `src/api/auth/auth.guard.ts`, registrado como guard global via `APP_GUARD` no `app.module`. Sempre permite; a segurança real (OIDC Keycloak/Zitadel, validação de JWT e extração do provedor do subject) substitui este guard sem tocar no use case.
* A submissão (`SubmitWagerTransactionUseCase`) e o consumer SQS passam o `providerId` pela porta antes de usar — a identidade dentro da mensagem continua sujeita às mesmas validações de domínio.
* `GET /health/live`, `GET /health/ready` e `GET /metrics` abertos, sem guard.
* Fila SQS tratada como canal interno confiável, mas identidade do provedor dentro da mensagem passa pelas mesmas validações de domínio (não confiamos cegamente).
* Desenho futuro: `AuthorizationCode + JWT do IdP`, `providerId = subject`, escopo por wallet; hoje o risco é assumido e isolado na borda.

## 7. Taxonomia de `failureCode` e HTTP

Toda `REJECTED` carrega `failureCode` estável legível por máquina. Taxonomia inicial travada (mínima, sem inventar):

| `failureCode` | Quando | HTTP | Reenviar? |
|---|---|---|---|
| `INSUFFICIENT_BALANCE` | `BET` sem saldo (cenário 100/80x2: a perdedora) | `422` | pode (se creditar antes) |
| `REVERSAL_WOULD_OVERDRAW` | `REFUND`/`ROLLBACK` que deixaria saldo negativo — distinto de aposta sem saldo | `422` | só após crédito |
| `REFERENCE_NOT_FOUND` | TTL/`PENDING_REFERENCE` esgotado | `422` | não, corrija referência |
| `INVALID_REFERENCE` | referência existe mas é `REJECTED`, ou de kind/wallet/player/moeda/round/provider errado | `422` | não, corrija payload |
| `ALREADY_REVERSED` | referência já revertida (única reversão por referência, qualquer tipo — ver §3) | `422` | não |
| `AMOUNT_MISMATCH` | valor ≠ valor da referência (parcial fora de escopo) | `422` | não, corrija valor |
| `CURRENCY_MISMATCH` | moeda da operação ≠ wallet | `422` | não |
| `WALLET_OWNER_MISMATCH` | `playerId` da operação ≠ dono da wallet | `422` | não, corrija payload |
| `IDEMPOTENCY_CONFLICT` | mesma key (ou mesma external id) com payload diferente | `409` | não, use nova key |
| `WALLET_CONFLICT` | `playerId+currency` duplicado | `409` | não |
| `WALLET_NOT_FOUND` | wallet inexistente | `404` | não |
| `TRANSACTION_NOT_FOUND` | transação inexistente nos GETs | `404` | não |
| `INVALID_PAYLOAD` | DTO/Money inválido (`NaN`, escala, `OPENING` externo etc.) | `400` | só após corrigir |
| `PENDING_REFERENCE` | aceite, aguardando referência (não é falha, é `status`) | `202` | aguarde worker |
| `INFRA_TRANSIENT` | PG/SQS fora, lock timeout `55P03`, deadlock `40P01` | `503` | sim, com backoff |

Consistência travada: `400` = borda inválida; `404` = wallet inexistente; `409` = conflito de identidade; `422` = regra de negócio violada (com `failureCode` específico, nunca só mensagem); `202` = aceito pendente; `503` = transitório. Todos os endpoints usam o mesmo filtro de exceção. `PROCESSED` novo (inclui `LOSS` sem ledger) → `201`; replay → `200` + `idempotentReplay: true`. `WALLET_NOT_FOUND` (`404`) não persiste nada; no SQS é terminal (ack) com log + métrica. `GETs` sempre respondem `200` com o envelope (qualquer status, inclusive `REJECTED`) — `422`/`409` existem só na resposta do `POST` de submissão.

## 8. Ledger, reconciliação, observabilidade e testes

**Ledger (travado):** `WalletLedgerEntry` imutável por construção (sem setters, `create` valida `before ± money == after`, `isBalanced()`), max 1 por `(transactionId, walletId)`, `LOSS` e `REJECTED` sem lançamento, sem `UPDATE/DELETE` (trigger `trg_ledger_immutable`), aritmética garantida pelo `CHECK ck_ledger_balanced`. `BET → DEBIT`, `WIN/REFUND → CREDIT`, `ROLLBACK → inverso da referência`. Sem double-entry (diferencial opcional, fora do escopo inicial).

**Reconciliação (fatia 8a):** `POST /wallets/:walletId/reconciliation` retrata saldo materializado e ledger com UMA única query SQL (snapshot consistente sob `READ COMMITTED`, sem lock da wallet quente e sem a corrida de ler wallet e ledger em dois passos), responde `{ storedBalance, calculatedBalance, difference, consistent, checkedEntries }` — `difference` é a aritmética do `NUMERIC` (texto de 2 casas, pode ser negativa; deliberadamente NÃO passa por `Money.from`). Divergência nunca é corrigida silenciosamente: log JSON `ERROR` com `walletId`, incrementa o contador `reconciliation_mismatches_total` via porta `MetricsPort` (implementação em memória, exposta em `/metrics`; §8) e responde `consistent: false`. `GET /wallets/:walletId/ledger?cursor=&limit=` pagina por keyset `(created_at, id) ASC` (índice `idx_ledger_wallet_created_id`, migration reversível da 8a); cursor opaco = base64url de `{ createdAt (ISO µs do PG), id }` (look-ahead `limit+1` decide o fim; `Date` perde 3 dígitos e quebraria o keyset no empate de ms); `limit` default `50` máx `200`; página vazia de wallet existente devolve `items: []`.

**Observabilidade (implementada, fatia 8b):**

* Logs: uma linha JSON por evento em stdout/stderr (`{ level, event|msg, timestamp, correlationId, messageId, transactionId, walletId, providerId }`). O logger do Nest (boot included) e todos os eventos da casa saem por `JsonLogger`/`logEvent` (`src/infra/observability/json-logger.ts`); o contexto de operação vive em `AsyncLocalStorage` (`log-context.ts`) — request HTTP abre escopo no middleware de correlação, mensagem SQS abre por `messageId` (`correlationId = messageId`, escolha única para traçar do recebimento à publicação do evento). Correlação HTTP: leitura/geração de `X-Correlation-Id` (trim, ≤200, sanitizada) com eco no header da resposta e propagação ao use case (propaga à outbox via `correlationId` do evento). `LOG_LEVEL` (`debug|info|error|silent`, default `info`) controla threshold. Nenhum valor monetário nos logs — a divergência da reconciliação loga só `walletId` + `difference`.
* Métricas: registry em memória (`MetricsPort`/`MemoryMetrics` — counter com labels, histograma e gauges computados no scrape), exposto por `GET /metrics` no text format Prometheus 0.0.4 escrito à mão (`prometheus.ts` — HELP/TYPE, `_bucket{le}` cumulativo, `_sum`, `_count`). Nomes canônicos (underscore): `wager_transactions_total{status,kind}` (replay não recomputa — duplicata vai pro contador certo), `wager_duplicates_total{source=idempotency|inbox}` (HTTP: replay do use case; SQS: `tryInsert` devolve `false`, contado só pós-commit), `sqs_retries_total` (redelivery com `ApproximateReceiveCount >= 2`), `db_lock_conflicts_total` (55P03/40P01 no `UnitOfWork.run` — incrementa e mantém o rethrow `503`), `outbox_publish_errors_total` (antes do `scheduleRetry`), `reconciliation_mismatches_total`, histograma `wager_processing_latency_ms` (buckets 5..5000 em volta do `execute` da submissão), gauges `sqs_dlq_messages` (`GetQueueAttributes` da DLQ com cache 5s) e `outbox_lag_seconds` (`now - min(occurred_at WHERE published_at IS NULL)`; 0 sem pendência).
* Health: `GET /health/live` (processo vivo, sem dependência) vs `GET /health/ready` (PG `SELECT 1` + SQS `GetQueueUrl` da fila configurada, 2s de prazo por check; 200 `{status:"ok",checks:{postgres:"up",sqs:"up"}}` / 503 mesma forma com `"down"`, sem dado sensível).

**Testes (travados, `bun test`; integração/concorrência com PG + LocalStack reais, zero mock de PG/SQS):**

* Unit: `Money` (escala, `NaN/Infinity/científica/vazio/>2 casas/negativo`, cross-currency), `Wallet` (negativo, `version` só em mudança de saldo), `BET/WIN/LOSS/REFUND/ROLLBACK`, moeda, idempotency-key com payload divergente.
* Integração: migrations/constraints, atomicidade wallet+ledger+inbox+outbox (mata processo antes/depois do commit), inbox+redelivery, publishers concorrentes na mesma outbox (`SKIP LOCKED` + lease), retry/DLQ, recovery pós-restart.
* Concorrência (paralelismo real): 50× replay → 1 débito; 100/80×2 → 1 `PROCESSED` + 1 `INSUFFICIENT_BALANCE`, saldo `20.00`, 1 ledger; wallets distintas em paralelo; ≥3 instâncias; kill após commit antes do ack; 2 publishers mesma outbox; `REFUND` antes da referência; restart com consistência final. Os dois testes obrigatórios (100/80×2 e 50× replay) pertencem à fatia 5b (Submit) — são o que prova lock e `ON CONFLICT`; sem eles a 5b não está pronta.
* **Invariante final de todos os testes: `wallet.balance == saldo reconstruído pelo ledger`.** Helper `assertLedgerConsistency(walletId)` roda no `afterEach` dos testes de concorrência.

## 9. Trade-offs, limitações e decisões resolvidas

**Trade-offs assumidos:**

1. Pessimista por wallet troca throughput de hot wallet por simplicidade e determinismo no `100/80x2`. Hot wallet vira fila sequencial no PG; aceito porque correção > RPS neste desafio.
2. `NUMERIC(19,2)` é mais lento que `BIGINT` de centavos, mas impõe escala e `CHECK` no schema e lê como `"25.00"` sem conversão manual.
3. FIFO (`MessageGroupId=walletId`) é só otimização de ordem; a garantia continua no banco (exigência da spec).
4. Sem double-entry e sem reversão parcial: `REFUND`/`ROLLBACK` exigem valor igual e única vez (qualquer tipo). Menos flexível, muito mais auditável.
5. Moeda única operacional (`BRL`) com modelo multi-moeda: reduz matriz de teste sem esconder `CURRENCY_MISMATCH`.

**Limitações conhecidas:**

* Sem auth real (guard no-op); sem rate-limit por provider; sem idempotência cross-moeda automática.
* `PENDING_REFERENCE` de 24h retém linhas; precisa de purga/arquivamento futuro.
* Outbox polling (500ms/50) tem lag mínimo; sem CDC (Debezium) nesta fase.
* Carga é diferencial opcional — só após o núcleo verde, se sobrar tempo.

**Decisões do dono (resolvidas antes de codar):**

1. `lock_timeout 3s` + `503` com retry. Sem fila interna por wallet (complexidade extra sem ganho de nota).
2. TTL 24h / 20 tentativas mantidos, configuráveis por env (`PENDING_REF_TTL_HOURS`, `PENDING_REF_MAX_ATTEMPTS`) para testes usarem TTL curto.
3. Só a fila SQS de eventos `wager-events.fifo`. Webhook fora de escopo.
4. `test:load` só no fim, se sobrar tempo.

## Pendências por fatia (do review pós-5a — não fazer antes da hora)

- ✓ Fatia 7 (mensageria): `GET /health/ready`, `InboxMessage` de domínio, tests de crash recovery via ganchos `FAULT_*` (commit/ack e claim/publicação, fatia 7d). Fatia 7d completa: consumer e publisher mortos entre commit/ack e claim/publicação (`crash-recovery`), carga multiprocesso com 3 instâncias `api,outbox,pending` + consumer SQS separado — 80/80, replay 50×, perna SQS com reenvios (`multiprocess-replay`) e SIGKILL com substituição + carga mista envolvendo `REFUND` antes da referência, exaustão de tentativas → `REJECTED REFERENCE_NOT_FOUND` e prova de eventos `outbox ⊆ fila de eventos` (`kill-restart`).
- ✓ Fatia 8a (consultas): `GET /wallets/:walletId/ledger` (keyset `(created_at, id)`, cursor base64url µs-preciso, limit 50/200) e `POST /wallets/:walletId/reconciliation` (query única READ COMMITTED, `MetricsPort` em memória + contador `reconciliation_mismatches_total`), com migration reversível do índice `idx_ledger_wallet_created_id` e testes HTTP de paginação/reconciliação.
- ✓ Fatia 8b (observabilidade): logs JSON de uma linha com correlationId via ALS (`LOG_LEVEL` env, logger do Nest incluso no boot), `GET /health/ready` com checks reais de Postgres e SQS (2s por check) e `GET /metrics` em formato Prometheus à mão (registry `MetricsPort` com counter/histograma/gauge; instrumentação da submissão, consumer, outbox, lock e reconciliação), com suíte de integração de processo real (spawn + carga HTTP + assert de stdout/contadores).
- Fatia 8 (polimento): limpeza de docblocks (AGENTS "só onde não óbvio"), regex de escala duplicado DTO×domínio, interpretação do `Money.equals` cross-currency (hoje retorna `false`; reavaliar contra a leitura literal da spec).
