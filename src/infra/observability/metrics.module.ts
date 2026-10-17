import { Global, Module } from '@nestjs/common';

import { MetricsController } from '../../api/metrics/metrics.controller';
import type { MetricsPort } from '../../application/ports';
import { METRICS } from '../../application/ports';
import { MemoryMetrics } from './metrics';

/**
 * Observabilidade (fatia 8b): `MetricsPort` com registry em memória (counter
 * com labels, histograma de latência, gauges no scrape) e a exposição HTTP em
 * `/metrics`. Registry é singleton por processo; Controller compartilha a
 * instância mesma — nenhum I/O na instrumentação da aplicação.
 */
@Global()
@Module({
  controllers: [MetricsController],
  providers: [{ provide: METRICS, useClass: MemoryMetrics }],
  exports: [METRICS],
})
export class MetricsModule {
  // Nada além do binding do token; a implementação vive em `metrics.ts`.
}
