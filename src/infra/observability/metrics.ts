import { Injectable } from '@nestjs/common';

import type { MetricsPort } from '../../application/ports';

/**
 * Contadores em memória (fatia 8a): singleton por processo (provider único no
 * módulo). A fatia 8b trará a exposição serializável; o nome de contador já é
 * o contrato definitivo (`reconciliation_mismatches_total`).
 */
@Injectable()
export class MemoryMetrics implements MetricsPort {
  private readonly counters = new Map<string, number>();

  increment(name: string): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + 1);
  }

  /** Leitura direta para testes (a 8b trará o formato de exposição). */
  counter(name: string): number {
    return this.counters.get(name) ?? 0;
  }
}
