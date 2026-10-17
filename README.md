# wagering-processor

Serviço financeiro distribuído que processa operações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) chegando por HTTP e SQS, com saldo, ledger e outbox atomicamente consistentes no mesmo commit SQL. Idempotência persistente, concorrência segura entre múltiplas instâncias e `wallet.balance == ledger reconstruído` sempre.

- Enunciado original (não editar): [`CHALLENGE.md`](./CHALLENGE.md)
- Decisões, trade-offs e limitações: [`ARCHITECTURE.md`](./ARCHITECTURE.md)

## Stack

Bun >= 1.4.3, TypeScript strict, NestJS, PostgreSQL 16, MikroORM 7 com migrations versionadas, SQS via LocalStack, Docker Compose e `decimal.js` para dinheiro.

## Pré-requisitos

- Bun >= 1.4.3
- Docker (CLI com suporte a `docker compose`)

## Como rodar

```bash
bun install
cp .env.example .env
docker compose up -d
bun run migration:up
bun run start
```

O Postgres sobe pelo compose na porta host 5433 e o LocalStack (SQS) na 4566. A API escuta na porta `PORT` (default `3000`). Todos os comandos citados neste README existem no `package.json` (`start`, `start:dev`, `test`, `test:crash`, `test:all`, `typecheck`, `migration:up`, `migration:down`).

## Endpoints

Os exemplos assumem a API em `http://localhost:3000`. Erros de negócio seguem o mesmo envelope JSON com `failureCode` (ver §7 do ARCHITECTURE.md).

Criar wallet (saldo inicial gera transação `OPENING` e lançamento `CREDIT` no ledger):

```bash
curl -X POST http://localhost:3000/wallets \
  -H 'content-type: application/json' \
  -d '{"playerId":"5714d30a-f7d9-4a3b-8c37-2ca4df67bd1a","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Submeter transação (`Idempotency-Key` é obrigatório; o hash do payload detecta conflito entre chaves diferentes):

```bash
curl -X POST http://localhost:3000/wagering/transactions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"5714d30a-f7d9-4a3b-8c37-2ca4df67bd1a","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Replay: repetir o mesmo curl acima devolve a mesma resposta com `idempotentReplay: true`. A mesma `Idempotency-Key` com payload diferente responde `409 IDEMPOTENCY_CONFLICT`.

Ledger paginado (keyset por cursor opaco; `limit` default 50, máximo 200):

```bash
curl 'http://localhost:3000/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/ledger?limit=50'
curl 'http://localhost:3000/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/ledger?cursor=<nextCursor>'
```

Reconciliação (snapshot do saldo contra o sum do ledger):

```bash
curl -X POST http://localhost:3000/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/reconciliation
```

Health (abertos, sem auth):

```bash
curl http://localhost:3000/health/live
curl http://localhost:3000/health/ready
```

Métricas em texto Prometheus:

```bash
curl http://localhost:3000/metrics
```

## Testes

```bash
bun run test        # unit + integração leve com Postgres real (~40s)
bun run test:crash  # multiprocesso, kill e restart com processos reais (~2min20s)
bun run test:all    # tudo, igual a bun test (mais lento)
```

Os testes de integração usam o banco `wagering_test`, criado e migrado automaticamente pelo preload (nunca o banco de desenvolvimento). Fila SQS privada por suíte e múltiplas instâncias reais do app vivem nos testes de `test/crash/`.

## Variáveis de ambiente

| Variável | Papel |
|---|---|
| `PORT` | Porta HTTP do processo (default 3000) |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Postgres; o compose expõe a porta host 5433 |
| `LOG_LEVEL` | `debug`, `info`, `error` ou `silent` (default `info`) |
| `WORKERS_ENABLED` | Papéis executados por este processo: `api`, `outbox`, `sqs`, `pending` (csv, default `api,outbox,sqs,pending`); instâncias podem rodar um papel cada |
| `OUTBOX_POLL_INTERVAL_MS`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE_MS` | Publisher da outbox (claim com lease e `SKIP LOCKED`) |
| `PENDING_REF_POLL_INTERVAL_MS`, `PENDING_REF_TTL_HOURS`, `PENDING_REF_MAX_ATTEMPTS` | Worker de `PENDING_REFERENCE` (backoff com jitter, TTL de 24h e teto de 20 tentativas) |
| `DB_LOCK_TIMEOUT_MS` | `SET LOCAL lock_timeout` das transações financeiras (default 3000) |
| `SQS_WAGER_QUEUE`, `SQS_EVENTS_QUEUE`, `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | SQS (LocalStack local: credenciais fake) |
| `FAULT_EXIT_AFTER_COMMIT`, `FAULT_EXIT_AFTER_CLAIM` | Ganchos de falha para os testes de crash recovery; só existem com `NODE_ENV=test` |
| `NODE_ENV` | Força `test` para os ganchos e defaults de teste |

## Estrutura de pastas

```text
src/
  domain/       # Money, Wallet, WagerTransaction, WalletLedgerEntry, InboxMessage, OutboxMessage, eventos (puro)
  application/  # use cases compartilhados entre HTTP e SQS, portas e validador único de entrada
  infra/        # entities MikroORM, migrations, repositórios, SQS consumer/publisher, workers, logs e métricas
  api/          # controllers NestJS, DTOs e mapeamento failureCode para HTTP
test/
  unit/         # domínio e aplicação sem infra
  integration/  # Postgres e LocalStack reais
  crash/        # multiprocesso, kill e restart com processos reais
  helpers/      # spawn de processos do app e filas SQS privadas por suíte
scripts/        # migrate.ts (migration:up/down)
infra/          # localstack-init.sh (cria as filas FIFO do compose)
```

## O que ficou fora

- Auth real: não implementada; existe o ponto de extensão `ProviderIdentityPort` + `AuthGuard` no-op (global) + `NoopProviderIdentity` (valida formato do `providerId`; identidade inválida vira `INVALID_PROVIDER` 422). Substituir o guard por OIDC não toca no use case (não vale pontos no desafio).
- `bun run test:load`: diferencial opcional do desafio, não implementado.
- Double-entry completo e CDC: fora do escopo inicial (ver trade-offs no ARCHITECTURE.md).
