import { Injectable, type CanActivate, type ExecutionContext } from '@nestjs/common';

/**
 * Guard no-op: auth real é deferida (não vale pontos). Sempre permite.
 * Onde entraria o OIDC: validar o JWT (Keycloak/Zitadel) e extrair o
 * provedor do subject, substituindo este guard por uma implementação real.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}
