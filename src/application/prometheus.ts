import type { MetricFamily } from './ports';

/** Escapa pelo exposition format (label values não contêm aspas no uso da casa). */
function escapeLabelValue(value: string): string {
  // Ordem importa: barra invertida primeiro, depois aspas e newline.
  return value.replaceAll('\\', '\\\\').replaceAll('"', '\\"').replaceAll('\n', '\\n');
}

const HELP_TEXT: Record<string, string> = {
  wager_transactions_total: 'Transações submetidas por status e tipo',
  wager_duplicates_total: 'Duplicatas detectadas por fonte',
  sqs_retries_total: 'Entregas SQS com ApproximateReceiveCount >= 2',
  db_lock_conflicts_total: 'Conflitos de lock (55P03/40P01) na UnitOfWork',
  outbox_publish_errors_total: 'Falhas de publicação da outbox (antes do retry agendado)',
  reconciliation_mismatches_total: 'Divergências de saldo vs ledger na reconciliação',
  wager_processing_latency_ms: 'Latência da submissão (execute do use case)',
  sqs_dlq_messages: 'Mensagens na DLQ (gauge com cache de 5s)',
  outbox_lag_seconds: 'Atraso do pending mais antigo da outbox (segundos)',
};

/** Prometheus text exposition format 0.0.4 — escrito à mão (nada de dependência extra). */
export function renderPrometheus(families: readonly MetricFamily[]): string {
  const lines: string[] = [];
  for (const family of families) {
    const help = HELP_TEXT[family.name];
    if (help !== undefined) {
      lines.push(`# HELP ${family.name} ${help}`);
    }
    lines.push(`# TYPE ${family.name} ${family.type}`);
    if (family.type === 'histogram') {
      for (const histogram of family.histograms ?? []) {
        const base = renderLabels(histogram.labels);
        for (const [le, count] of histogram.bucketCounts) {
          lines.push(
            `${family.name}_bucket${mergeLabel(histogram.labels, 'le', String(le))} ${count}`,
          );
        }
        lines.push(`${family.name}_bucket${mergeLabel(histogram.labels, 'le', '+Inf')} ${histogram.count}`);
        lines.push(`${family.name}_sum${base} ${formatValue(histogram.sum)}`);
        lines.push(`${family.name}_count${base} ${histogram.count}`);
      }
      continue;
    }
    for (const sample of family.samples ?? []) {
      lines.push(`${family.name}${renderLabels(sample.labels)} ${formatValue(sample.value)}`);
    }
  }
  return `${lines.join('\n')}\n`;
}

function formatValue(value: number): string {
  // Floats inteiros e NaN nunca aparecem (gauge invalidado não entra no snapshot).
  return Number.isInteger(value) ? String(value) : value.toFixed(6);
}

function renderLabels(labels: Record<string, string>): string {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) {
    return '';
  }
  const inner = keys
    .map((key) => `${key}="${escapeLabelValue(labels[key] ?? '')}"`)
    .join(',');
  return `{${inner}}`;
}

function mergeLabel(labels: Record<string, string>, key: string, value: string): string {
  const rest = Object.fromEntries(Object.entries(labels).filter(([name]) => name !== key));
  const base = renderLabels(rest);
  const inner = `${key}="${escapeLabelValue(value)}"`;
  if (base === '') {
    return `{${inner}}`;
  }
  return `{${base.slice(1, -1)},${inner}}`;
}
