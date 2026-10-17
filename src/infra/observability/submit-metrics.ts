import { logEvent } from './json-logger';
import {
  WAGER_DUPLICATES_TOTAL,
  WAGER_TRANSACTIONS_TOTAL,
} from '../../application/metric-names';
import type { MetricsPort } from '../../application/ports';
import type { SubmitWagerCommands } from '../../application/submit-wager.use-case';
import type {
  SubmitWagerContext,
  SubmitWagerInput,
  SubmitWagerResult,
} from '../../application/submit-wager.use-case';

const COUNTED_OUTCOMES = ['PROCESSED', 'REJECTED', 'PENDING_REFERENCE'] as const;

/**
 * Decorador da submissão: instrumenta SEM tocar no use case puro — counter de
 * transação por status/kind, counter de duplicata (HTTP: idempotency; SQS tem
 * a própria fonte no consumer) e histograma de latência em volta de `execute`.
 * Também emite o log de resultado (`wager.submitted`) com os identificadores.
 */
export class InstrumentedSubmitWagerUseCase {
  constructor(
    private readonly inner: SubmitWagerCommands,
    private readonly registry: MetricsPort,
  ) {}

  async execute(input: SubmitWagerInput, context?: SubmitWagerContext): Promise<SubmitWagerResult> {
    const started = performance.now();
    try {
      const result = await this.inner.execute(input, context);
      if (
        (COUNTED_OUTCOMES as readonly string[]).includes(result.outcome) &&
        'transaction' in result
      ) {
        if (result.replay === true) {
          // Replay não é transação nova: o counter de transações não cresce.
          // HTTP: dedupe pela Idempotency-Key; na borda SQS a fonte é o inbox
          // (contado após o tryInsert no consumer, com commit garantido).
          if (this.isHttpPath(context)) {
            this.registry.increment(WAGER_DUPLICATES_TOTAL, { source: 'idempotency' });
          }
        } else {
          this.registry.increment(WAGER_TRANSACTIONS_TOTAL, {
            status: result.outcome,
            kind: input.kind,
          });
        }
        logEvent('info', 'wager.submitted', {
          outcome: result.outcome,
          transactionId: result.transaction.id,
          walletId: input.walletId,
          providerId: input.providerId,
          ...(input.correlationId !== undefined ? { correlationId: input.correlationId } : {}),
        });
      }
      return result;
    } finally {
      this.registry.observe('wager_processing_latency_ms', performance.now() - started);
    }
  }

  /** HTTP puro: SEM `beforeRepositories` (inbox SQS) e SEM resume (worker). */
  private isHttpPath(context?: SubmitWagerContext): boolean {
    return context?.beforeRepositories === undefined && context?.pendingResolution === undefined;
  }
}
