import { Controller, Get, Inject, Res } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';

import type { MetricsPort } from '../../application/ports';
import { METRICS } from '../../application/ports';
import { renderPrometheus } from '../../application/prometheus';

/** Mínimo do Express usado aqui; evita depender dos tipos do framework. */
interface MetricsResponse {
  setHeader(name: string, value: string): unknown;
}

/**
 * Exposição Prometheus (`GET /metrics`, CHALLENGE §12): o texto vem do
 * snapshot do registry em memória; os gauges são computados só no scrape.
 */
@Controller()
export class MetricsController {
  constructor(
    @Inject(METRICS) private readonly registry: MetricsPort,
    private readonly orm: MikroORM,
  ) {
    // Gauge computado no scrape (outbox pendente mais antiga; 0 sem pendência).
    this.registry.setGauge('outbox_lag_seconds', () => this.outboxLagSeconds());
  }

  @Get('metrics')
  async metrics(@Res({ passthrough: true }) res: MetricsResponse): Promise<string> {
    res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    return renderPrometheus(await this.registry.snapshot());
  }

  private async outboxLagSeconds(): Promise<number> {
    const rows = await this.orm.em.getConnection().execute<Array<{ lag: number | string }>>(
      `SELECT COALESCE(EXTRACT(epoch FROM (now() - min(occurred_at))), 0)::float8 AS lag
         FROM outbox_message
        WHERE published_at IS NULL`,
    );
    return Number(rows[0]?.lag ?? 0);
  }
}
