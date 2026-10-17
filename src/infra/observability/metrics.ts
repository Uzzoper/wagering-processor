import { Injectable } from '@nestjs/common';

import type {
  MetricFamily,
  MetricHistogram,
  MetricLabels,
  MetricSample,
  MetricsPort,
} from '../../application/ports';

/** Buckets do histograma de latência (ms), sugeridos pelo orquestrador. */
export const LATENCY_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

/** Chave estável de série: labels ordenados — a ordem de inserção não muda o texto. */
function seriesKey(labels: MetricLabels | undefined): string {
  if (labels === undefined) {
    return '';
  }
  return Object.keys(labels)
    .sort()
    .map((key) => `${key}\u0000${JSON.stringify(labels[key])}`)
    .join('\u0001');
}

interface CounterSeries {
  labels: MetricLabels;
  value: number;
}

interface HistogramSeriesData {
  labels: MetricLabels;
  values: number[];
}

/** Escapar pelo exposition format (label values nunca contêm aspas no nosso uso). */
function escapeLabelValue(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll('"', '\\"').replaceAll('\n', '\\n');
}

/**
 * Registry em memória (fatia 8b): singleton por processo (provider único no
 * módulo). Counter e histograma são incrementados por instrução; os gauges são
 * callbacks computados no scrape — nada aqui faz I/O por si só.
 */
@Injectable()
export class MemoryMetrics implements MetricsPort {
  private readonly counters = new Map<string, CounterSeries>();
  private readonly histograms = new Map<string, { buckets: readonly number[]; series: Map<string, HistogramSeriesData> }>();
  private readonly gauges = new Map<string, { labels: MetricLabels | undefined; read: () => Promise<number> | number }>();

  increment(name: string, labels?: MetricLabels): void {
    const key = seriesKey(labels);
    const mapKey = `${name}\u0000${key}`;
    const current = this.counters.get(mapKey);
    if (current === undefined) {
      this.counters.set(mapKey, { labels: labels ?? {}, value: 1 });
      return;
    }
    current.value += 1;
  }

  counter(name: string, labels?: MetricLabels): number {
    return this.counters.get(`${name}\u0000${seriesKey(labels)}`)?.value ?? 0;
  }

  observe(name: string, valueMs: number): void {
    let family = this.histograms.get(name);
    if (family === undefined) {
      family = { buckets: LATENCY_BUCKETS_MS, series: new Map() };
      this.histograms.set(name, family);
    }
    const key = seriesKey(undefined);
    let series = family.series.get(key);
    if (series === undefined) {
      series = { labels: {}, values: [] };
      family.series.set(key, series);
    }
    series.values.push(valueMs);
  }

  setGauge(name: string, read: () => Promise<number> | number): void {
    this.gauges.set(name, { labels: undefined, read });
  }

  async snapshot(): Promise<readonly MetricFamily[]> {
    const families: MetricFamily[] = [];

    for (const [mapKey, series] of this.counters) {
      const [name] = mapKey.split('\u0000');
      const samples: MetricSample[] = [{ labels: series.labels, value: series.value }];
      this.pushOrMerge(families, name!, 'counter', { samples });
    }

    for (const [name, gauge] of this.gauges) {
      try {
        const value = await gauge.read();
        if (Number.isFinite(value)) {
          this.pushOrMerge(families, name, 'gauge', { samples: [{ labels: gauge.labels ?? {}, value }] });
        }
      } catch {
        // Gauge indisponível (ex.: SQS fora): a família simplesmente não aparece.
      }
    }

    for (const [name, family] of this.histograms) {
      const buckets = family.buckets;
      const histograms: MetricHistogram[] = [];
      for (const series of family.series.values()) {
        const sorted = [...series.values];
        histograms.push({
          labels: series.labels,
          buckets,
          bucketCounts: buckets.map((le) => [le, sorted.filter((value) => value <= le).length]),
          sum: sorted.reduce((total, value) => total + value, 0),
          count: sorted.length,
        });
      }
      this.pushOrMerge(families, name, 'histogram', { histograms });
    }

    return families;
  }

  private pushOrMerge(
    families: MetricFamily[],
    name: string,
    type: MetricFamily['type'],
    patch: { samples?: MetricSample[]; histograms?: MetricHistogram[] },
  ): void {
    const existing = families.find((family) => family.name === name);
    if (existing !== undefined) {
      if (patch.samples?.length) {
        (existing.samples as MetricSample[]).push(...patch.samples);
      }
      if (patch.histograms?.length) {
        (existing.histograms as MetricHistogram[]).push(...patch.histograms);
      }
      return;
    }
    families.push({ name, type, samples: patch.samples, histograms: patch.histograms });
  }
}

/** Names de série com labels ordenados reusado pela exposição. */
export { seriesKey, escapeLabelValue };
