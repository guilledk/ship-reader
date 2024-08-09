import { z } from 'zod';

export const BlockDescriptorSchema = z.object({
    block_num: z.number(),
    block_id: z.string(),
});

export type BlockDescriptor = z.infer<typeof BlockDescriptorSchema>;

export const SyncStatusSchema = z.object({
    head: BlockDescriptorSchema,
    last_irreversible: BlockDescriptorSchema,
    this_block: BlockDescriptorSchema,
    prev_block: BlockDescriptorSchema,
});

export type SyncStatus = z.infer<typeof SyncStatusSchema>;

export const BlockHeaderSchema = z.object({
    timestamp: z.string(),
    producer: z.string(),
    confirmed: z.number(),
    previous: z.string(),
    transaction_mroot: z.string(),
    action_mroot: z.string(),
    schedule_version: z.number(),
    new_producers: z.any(),
    header_extensions: z.array(z.any()),
    producer_signature: z.string(),
    block_extensions: z.array(z.any()),
});

export type BlockHeader = z.infer<typeof BlockHeaderSchema>;

export const AuthorizationSchema = z.object({
    actor: z.string(),
    permission: z.string(),
});

export type Authorization = z.infer<typeof AuthorizationSchema>;

export const ActSchema = z.object({
    account: z.string(),
    name: z.string(),
    authorization: z.array(AuthorizationSchema),
    data: z.any(),
});

export type Act = z.infer<typeof ActSchema>;

export const AuthSequenceSchema = z.object({
    account: z.string(),
    sequence: z.number(),
});

export type AuthSequence = z.infer<typeof AuthSequenceSchema>;

export const ReceiptSchema = z.object({
    receiver: z.string(),
    act_digest: z.string(),
    global_sequence: z.string(),
    recv_sequence: z.number(),
    auth_sequence: z.array(AuthSequenceSchema),
    code_sequence: z.number(),
    abi_sequence: z.number(),
});

export type Receipt = z.infer<typeof ReceiptSchema>;

export const ActionSchema = z.object({
    global_sequence: z.string(),
    action_ordinal: z.number(),
    creator_action_ordinal: z.number(),
    trx_id: z.string(),
    cpu: z.number(),
    net: z.number(),
    ram: z.array(z.any()),
    receipt: ReceiptSchema,
    receiver: z.string(),
    console: z.string(),
    signatures: z.array(z.any()),
    act: ActSchema,
});

export type Action = z.infer<typeof ActionSchema>;

export const DeltaSchema = z.object({
    code: z.string(),
    table: z.string(),
    delta: z.any()
});

export type Delta = z.infer<typeof DeltaSchema>;

export const BlockSchema = z.object({
    status: SyncStatusSchema,
    header: BlockHeaderSchema,
    deltas: z.array(DeltaSchema),
    actions: z.array(ActionSchema)
});

export type Block = z.infer<typeof BlockSchema>;

export const StateHistoryReaderOptionsSchema = z.object({
    shipAPI: z.string(),
    chainAPI: z.string(),
    irreversibleOnly: z.boolean().optional().default(false),
    blockHistorySize: z.number().optional(),
    startBlock: z.number().optional().default(-1),
    stopBlock: z.number().optional().default(-1),
    logLevel: z.string().optional().default('warning'),
    maxPayloadMb: z.number().optional().default(256),
    maxMsgsInFlight: z.number().optional().default(1000),
    fetchBlock: z.boolean().optional().default(true),
    fetchTraces: z.boolean().optional().default(true),
    fetchDeltas: z.boolean().optional().default(true),
    actionWhitelist: z.record(z.string(), z.array(z.string())).optional(),
    tableWhitelist: z.record(z.string(), z.array(z.string())).optional(),
    speedMeasureConf: z.object({
        windowSizeMs: z.number().optional().default(10000),
        deltaMs: z.number().optional().default(1000),
    }).optional(),
}).transform((options) => ({
    ...options,
    actionWhitelist: options.actionWhitelist
        ? new Map(Object.entries(options.actionWhitelist))
        : new Map(),
    tableWhitelist: options.tableWhitelist
        ? new Map(Object.entries(options.tableWhitelist))
        : new Map(),
}));

export type StateHistoryReaderOptions = z.infer<typeof StateHistoryReaderOptionsSchema>;
