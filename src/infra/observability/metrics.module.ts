import { Global, Module } from '@nestjs/common';

import type { MetricsPort } from '../../application/ports';
import { METRICS } from '../../application/ports';
import { MemoryMetrics } from './metrics';

/**
 * Observabilidade mínima da 8a: `MetricsPort` com contador em memória.
 * A 8b troca/adiciona a exposição HTTP sem tocar os use cases.
 */
@Global()
@Module({
  providers: [{ provide: METRICS, useClass: MemoryMetrics }],
  exports: [METRICS],
})
export class MetricsModule {
  // Nada além do binding do token; a implementação vive em `metrics.ts`.
}
