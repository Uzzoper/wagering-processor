import { Injectable, type PipeTransform } from '@nestjs/common';

import type { SubmitWagerBody } from './submit-wager.dto';
import { parseSubmitWagerBody } from './submit-wager.dto';

/** Pipe de validação do `POST /wagering/transactions`; lança `INVALID_PAYLOAD` na borda. */
@Injectable()
export class SubmitWagerPipe implements PipeTransform<unknown, SubmitWagerBody> {
  transform(value: unknown): SubmitWagerBody {
    return parseSubmitWagerBody(value);
  }
}
