import { InvalidProviderIdentityError } from '../../domain/errors';
import type { ProviderIdentityPort } from '../../application/ports';

/**
 * Implementação no-op da porta de identidade: aceita o `providerId` do payload
 * e valida apenas o formato (não vazio, tamanho máximo). Ponto de extensão
 * para OIDC (Keycloak/Zitadel): trocar esta classe por uma que valide o JWT
 * e extraia o provedor do subject, sem tocar no use case.
 */
export class NoopProviderIdentity implements ProviderIdentityPort {
  private static readonly MAX_LENGTH = 128;

  resolve(providerId: string): string {
    if (typeof providerId !== 'string' || providerId.trim().length === 0) {
      throw new InvalidProviderIdentityError(providerId);
    }
    if (providerId.length > NoopProviderIdentity.MAX_LENGTH) {
      throw new InvalidProviderIdentityError(providerId);
    }
    return providerId;
  }
}
