import type { Clock } from '../../application/ports';

/** Clock real do sistema (default em toda a produção). */
export const SYSTEM_CLOCK: Clock = { now: () => new Date() };
