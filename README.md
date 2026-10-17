# wagering-processor

Serviço financeiro distribuído do desafio Jungle Gaming — processamento de apostas com correção financeira, concorrência multi-instância, idempotência persistente e ledger auditável.

- Desafio original (não editar): [`CHALLENGE.md`](./CHALLENGE.md)
- Decisões técnicas: [`ARCHITECTURE.md`](./ARCHITECTURE.md) *(a criar)*
- Guia para agentes: [`AGENTS.md`](./AGENTS.md)

## Stack

Bun 1.x + NestJS + TypeScript strict + PostgreSQL + SQS (LocalStack) + Docker Compose + MikroORM (preferencial) + `decimal.js` para dinheiro.

## Como começar

```bash
bun install
bun run index.ts
```

> Projeto ainda em scaffold inicial (`bun init`). Próximos passos: Docker Compose (PG + LocalStack), Nest + MikroORM + migrations, domínio puro, HTTP, SQS/outbox, concorrência, observabilidade.

## Comandos (alvo)

```bash
bun install
bun run start
bun run test        # unit + integração leve (~34s)
bun run test:crash  # multiprocesso, kill e restart (~142s)
bun run test:all    # tudo, igual a `bun test` (~175s)
bun run migration:up / migration:down
bun run test:load # diferencial opcional
```

## Estrutura alvo

```
src/
  domain/       # Money, Wallet, WagerTransaction, Ledger, Inbox, Outbox, Events (puros, sem ORM/Nest)
  application/  # use-cases (mesmo para HTTP e SQS)
  infra/        # MikroORM entities, migrations, SQS, outbox workers, métricas/logs
  api/          # controllers Nest, DTOs, guards
test/
  unit/
  integration/  # PG + LocalStack reais
  crash/        # multiprocesso, kill e restart (processos reais)
  helpers/      # spawn de processos do app e filas SQS privadas por suíte
```

## Regras inegociáveis

Sem `number` para dinheiro, sem idempotência em memória, sem publicar evento antes do commit, sem lock global, sem `read-calculate-update` sem lock, constraints no banco, `wallet.balance == ledger reconstruído` sempre.
