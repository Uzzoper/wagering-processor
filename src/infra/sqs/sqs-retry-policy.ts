/** Backoff exponencial determinístico para falhas transitórias do consumer SQS. */
export function sqsRetryDelaySeconds(
  receiveCount: number,
  baseDelaySeconds = 30,
  maxDelaySeconds = 3_600,
): number {
  if (!Number.isInteger(receiveCount) || receiveCount < 1) {
    throw new RangeError('receiveCount must be a positive integer');
  }
  if (!Number.isInteger(baseDelaySeconds) || baseDelaySeconds < 1) {
    throw new RangeError('baseDelaySeconds must be a positive integer');
  }
  if (
    !Number.isInteger(maxDelaySeconds)
    || maxDelaySeconds < baseDelaySeconds
    || maxDelaySeconds > 43_200
  ) {
    throw new RangeError('maxDelaySeconds must be between baseDelaySeconds and 43200');
  }

  let delaySeconds = baseDelaySeconds;
  for (let attempt = 1; attempt < receiveCount && delaySeconds < maxDelaySeconds; attempt += 1) {
    delaySeconds = Math.min(delaySeconds * 2, maxDelaySeconds);
  }
  return delaySeconds;
}
