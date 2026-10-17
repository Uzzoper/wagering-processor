/**
 * Política do worker de `PENDING_REFERENCE` (ARCHITECTURE §5 / CHALLENGE §7.1).
 * TTL 24h e máx. 20 tentativas cobrem atraso operacional do provedor sem virar
 * lixo eterno; teto de backoff de 1h cabe dentro do TTL sem spam.
 */

export const PENDING_REF_BACKOFF_BASE_MS = 30_000;
export const PENDING_REF_BACKOFF_CAP_MS = 3_600_000;

/** Backoff exponencial com jitter uniforme em [goal/2, goal] (evita thundering herd). */
export function pendingRefBackoffMs(
  attempts: number,
  baseMs: number = PENDING_REF_BACKOFF_BASE_MS,
  capMs: number = PENDING_REF_BACKOFF_CAP_MS,
): number {
  const goal = Math.min(capMs, baseMs * 2 ** (attempts - 1));
  return Math.round(goal / 2 + Math.random() * (goal / 2));
}

/** Expirou por TTL (24h desde o primeiro defer) ou por tentativas esgotadas. */
export function pendingRefExpired(
  pendingSince: Date,
  retryAttempts: number,
  now: Date,
  ttlHours: number,
  maxAttempts: number,
): boolean {
  return (
    now.getTime() - pendingSince.getTime() >= ttlHours * 3_600_000 ||
    retryAttempts >= maxAttempts
  );
}
