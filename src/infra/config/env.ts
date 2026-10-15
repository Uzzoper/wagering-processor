/**
 * Leitura de configuração a partir de variáveis de ambiente.
 * Bun carrega `.env` automaticamente; aqui só centralizamos os nomes e defaults.
 * Nenhum segredo é logado — apenas host/porta/database são não-sensíveis o suficiente para log.
 */

export interface DatabaseConfig {
  host: string;
  port: number;
  name: string;
  user: string;
  password: string;
}

export interface AppConfig {
  port: number;
  database: DatabaseConfig;
}

function parsePort(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Porta inválida em variável de ambiente: "${value}"`);
  }
  return parsed;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    port: parsePort(env.PORT, 3000),
    database: {
      host: env.DB_HOST ?? 'localhost',
      port: parsePort(env.DB_PORT, 5433),
      name: env.DB_NAME ?? 'wagering',
      user: env.DB_USER ?? 'wagering',
      password: env.DB_PASSWORD ?? 'wagering',
    },
  };
}
