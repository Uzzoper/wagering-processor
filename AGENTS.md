# AGENTS.md — Guia para agentes neste repo

## Documentos fonte

- `CHALLENGE.md`: cópia fiel do desafio — **não editar**, é a spec.
- `README.md`: visão do projeto e comandos.
- `ARCHITECTURE.md`: decisões, trade-offs, limitações (criar ao implementar).

## Stack e comandos

- Bun 1.x: `bun install`, `bun <file>`, `bun test`, `bun run <script>`, `bunx`. Não usar node/npm/vite/express. Bun carrega `.env` sozinho.
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

## Onde mexer

- `src/domain/`: puro, testável sem infra.
- `src/application/`: use-case compartilhado HTTP+SQS.
- `src/infra/`: entities MikroORM, migrations reversíveis, SQS/inbox/outbox workers, logs/métricas.
- `src/api/`: controllers, DTOs com `MoneyProps` string.
- `test/unit|integration|concurrency/`: integração/concorrência com PG+SQS reais, invariante final `wallet.balance == ledger`.

## Testes e observabilidade

- `bun test` para tudo. Concurrency com paralelismo real (50x replay, 100/80x2, ≥3 instâncias, kill após commit antes do ack, REFUND antes da ref).
- Logs JSON com `correlationId/messageId/transactionId/walletId/providerId`, sem payload financeiro completo. Métricas: status, duplicatas, retries, DLQ, lock conflicts, outbox lag, latência. Health `live` vs `ready`.

## Estilo

Mudanças pequenas e justificadas, sem refatorar design alheio. Copy PT-BR simples. Não criar docs novas sem pedido.
