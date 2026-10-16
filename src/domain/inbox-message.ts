/**
 * Mensagem recebida do SQS (CHALLENGE §6.5). A linha `inbox_message` é o dedup
 * persistente do consumidor: nasce de `receive` e entra na MESMA tx do use case;
 * o commit marca o consumo como efetivo (por isso `processed_at` é preenchido
 * antes do INSERT — a linha só sobrevive junto com o efeito financeiro).
 */
export interface InboxMessageState {
  consumerName: string;
  messageId: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

export interface ReceiveInboxProps {
  consumerName: string;
  messageId: string;
  payloadHash: string;
  /** Default: `new Date()`. */
  receivedAt?: Date;
  /** Default: ausente (`isProcessed() === false`). */
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    public readonly consumerName: string,
    public readonly messageId: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage(
      props.consumerName,
      props.messageId,
      props.payloadHash,
      props.receivedAt ?? new Date(),
      props.processedAt,
    );
  }

  /** Reconstrução a partir da persistência — não revalida. */
  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.consumerName,
      state.messageId,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  markProcessed(at: Date): void {
    this._processedAt = at;
  }
}
