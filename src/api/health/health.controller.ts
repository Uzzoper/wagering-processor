import { Controller, Get } from '@nestjs/common';

export interface LivenessResponse {
  status: 'ok';
}

/**
 * Liveness: apenas indica que o processo está vivo.
 * Sem guard e sem dependência de banco/SQS.
 */
@Controller('health')
export class HealthController {
  @Get('live')
  live(): LivenessResponse {
    return { status: 'ok' };
  }
}
