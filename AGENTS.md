# AGENTS.md — Guia para agentes neste repo

## Documentos fonte

- `CHALLENGE.md`: cópia fiel do desafio — **não editar**, é a spec.
- `README.md`: visão do projeto e comandos (pode ser atualizado).
- `ARCHITECTURE.md`: fonte das decisões, leia antes de qualquer tarefa. Se uma decisão mudar, atualize o documento no mesmo commit.

## Stack e comandos

- Bun >= 1.4.3: `bun install`, `bun <file>`, `bun test`, `bun run <script>`, `bunx`. Não usar node/npm/vite. Não usar Express direto, só pela plataforma do Nest. Bun carrega `.env` sozinho.
- TS strict, NestJS, PostgreSQL, SQS via LocalStack, Docker Compose, MikroORM preferencial (TypeORM aceito, Prisma proibido), `decimal.js` para dinheiro.

## Regras de domínio (não quebrar)

1. Dinheiro só como `Money` imutável (`amount: string` 2 casas + `currency`), nunca `number/float`. Validar `NaN/Infinity/notação científica/vazio/>2 casas/negativo em input`. Cross-currency = erro. Domínio puro, sem ORM/Nest.
2. Construtor `private/protected` + `create/from/rehydrate` (`rehydrate` sem revalidação).
3. `Wallet`: única por `playerId+currency`, saldo nunca negativo, `version` inicia 1 e só incrementa quando saldo muda, `debit/credit` com ledger correspondente.
4. `WagerTransaction`: terminais `PROCESSED/REJECTED/FAILED` imutáveis. `OPENING` interno. Mesma idempotency-key + payload diferente = conflito.
5. `WalletLedgerEntry`: imutável, `create` valida `before ± money == after`. `LOSS`/`REJECTED` sem lançamento. Max 1 por wallet por tx. Nunca sobrescrever/excluir.
6. Inbox + saldo + ledger + outbox na **mesma tx SQL**. Ack SQS só pós-commit. Não publicar antes do commit.
7. Unidade de concorrência = `walletId`. Sem lock global, sem `read-calculate-update` sem lock. Constraints de unicidade/imutabilidade/não-negatividade no schema.
8. Auth não vale pontos: usar guard no-op / `ProviderIdentityPort`, documentar. Health aberto.
9. Rejeição de negócio nunca lança exceção: o use case RETORNA `PROCESSED`, `REJECTED` ou `PENDING_REFERENCE`, a transação faz commit (persistindo a linha `REJECTED` e o evento) e só a borda mapeia para HTTP. Exceção é só para erro de infraestrutura (rollback, 503).
10. Nunca capturar 23505 dentro da transação (aborta a tx). Usar `INSERT ... ON CONFLICT DO NOTHING RETURNING` e ler a linha existente.
11. Ordem dentro da tx: inbox (só SQS), insert idempotente, lock da wallet (`FOR UPDATE`), regra de negócio, persistir tudo, commit. `SET LOCAL lock_timeout = '3s'` no início.
12. Uma única reversão por referência, qualquer tipo (`REFUND` ou `ROLLBACK`), garantida pelo índice `uq_wager_single_reversal`.
13. Outbox: claim com lease (`next_attempt_at` para frente) e `FOR UPDATE SKIP LOCKED`. Publicar só depois do commit.
14. Worker PENDING_REFERENCE: reusa o use case de submissão via `pendingResolution` (resume, sem replay 202); renovação = attempts+1, backoff exponencial com jitter (30s→teto 1h), sem reemitir WagerTransactionPendingReference; exaustão TTL/tentativa → REJECTED REFERENCE_NOT_FOUND + evento Rejected; claim por lease + SKIP LOCKED igual à outbox.

## Onde mexer

- `src/domain/`: puro, testável sem infra.
- `src/application/`: use-case compartilhado HTTP+SQS.
- `src/infra/`: entities MikroORM, migrations reversíveis, SQS/inbox/outbox workers, logs/métricas.
- `src/api/`: controllers, DTOs com `MoneyProps` string.
- `test/unit|integration|crash/`: integração com PG+SQS reais; suítes multiprocesso/kill/restart vivem em `test/crash/`; invariante final `wallet.balance == ledger`.

## Testes e observabilidade

- Scripts de teste (tempos medidos nesta máquina): `bun run test` — unit + integração leve, ~34s; `bun run test:crash` — multiprocesso, kill e restart com processos reais, ~142s; `bun run test:all` — tudo, igual a `bun test`, ~175s. Paralelismo real: 50x replay, 100/80x2, ≥3 instâncias, kill após commit antes do ack, REFUND antes da ref.
- Testes de integração usam o banco `wagering_test` (nunca o de desenvolvimento). O schema sobe uma vez no preload; nenhum teste faz `down()` nem `TRUNCATE`. Cada teste usa UUIDs próprios.
- Postgres do compose na porta 5433.
- Antes de qualquer commit: `bun run typecheck` e `bun test` verdes.
- Logs JSON com `correlationId/messageId/transactionId/walletId/providerId`, sem payload financeiro completo. Métricas: status, duplicatas, retries, DLQ, lock conflicts, outbox lag, latência. Health `live` vs `ready`.

## Estilo e fluxo

- Comentários em português, uma ou duas linhas, só onde houver decisão não óbvia. Sem docblock em tudo.
- Commits atômicos em português (um assunto por commit). Não fazer push sem pedido.
- Não alterar `tsconfig.json` sem pedir (`verbatimModuleSyntax` e `useDefineForClassFields` ficam `false`). Não adicionar dependências sem avisar.
- Migration já aplicada é imutável: mudança de schema = migration nova.
- Mudanças pequenas e justificadas, sem refatorar o que não foi pedido.
- Não criar docs novas sem pedido; `README` e `ARCHITECTURE.md` podem ser atualizados.
