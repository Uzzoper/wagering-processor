import { Injectable, type PipeTransform } from '@nestjs/common';

import type { CreateWalletBody } from './create-wallet.dto';
import { parseCreateWalletBody } from './create-wallet.dto';

/** Pipe de validação do `POST /wallets`; lança `INVALID_PAYLOAD` na borda. */
@Injectable()
export class CreateWalletPipe implements PipeTransform<unknown, CreateWalletBody> {
  transform(value: unknown): CreateWalletBody {
    return parseCreateWalletBody(value);
  }
}
