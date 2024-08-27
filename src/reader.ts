import WebSocket, {RawData} from "ws";
import fetch from "node-fetch";
import * as console from "console";

import {ActionSchema, Block, BlockSchema, DeltaSchema, StateHistoryReaderOptions} from "./types.js";
import {addOnBlockToABI, ThroughputMeasurer} from "./utils.js";

import {ABI, ABIDecoder, APIClient, Name, Serializer} from "@wharfkit/antelope";
import {cargo, QueueObject} from "async";

import {createLogger, format, Logger, transports} from 'winston';
import * as process from "node:process";


export class StateHistoryReader {

    readonly options: StateHistoryReaderOptions;

    ws: WebSocket;
    reconnectCount: number = 0;

    startBlock: number;
    stopBlock: number;

    private stopped: boolean = true;  // flag to indicate reader can be started
    private connecting: boolean = false;  // flag to block more than one .start at a time
    private restarting: boolean = false;  // flag to block more than one .restart at a time

    private shipAbi?: ABI;
    private shipAbiReady = false;

    private contracts: Map<string, ABI> = new Map();
    private readonly api: APIClient;

    private inputQueue: QueueObject<any>;
    private nextSequence: number;  // next block we send should have this sequence number

    perfMetrics: ThroughputMeasurer;
    readonly speedMeasureWindowSize: number;
    readonly speedMeasureDeltaMs: number;

    readonly logger: Logger;

    private blocksSinceLastMeasure: number = 0;
    private readonly _perfMetricTask: NodeJS.Timeout;

    onConnected: () => void = null;
    onDisconnect: () => void = null;
    onError: (err: Error) => void = null;

    onBlock: (block: Block) => Promise<void> = null;
    onStop: (lastBlock: Block) => void = null;

    constructor(options: StateHistoryReaderOptions, logger?: Logger) {
        this.options = options;

        if (typeof logger === 'undefined') {
            const loggingOptions = {
                exitOnError: false,
                level: this.options.logLevel,
                format: format.combine(
                    format.metadata(),
                    format.colorize(),
                    format.timestamp(),
                    format.printf((info: any) => {
                        return `${info.timestamp} [PID:${process.pid}] [${info.level}] [READER] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                    })
                )
            }
            logger = createLogger(loggingOptions);
            logger.add(new transports.Console({
                level: this.options.logLevel
            }));
        }

        this.logger = logger;

        this.api = new APIClient({
            url: options.chainAPI,
            fetch
        });

        this.startBlock = options.startBlock ?? -1;
        this.stopBlock = options.stopBlock;
        this.nextSequence = 1;

        this.speedMeasureDeltaMs = 1000;
        this.speedMeasureWindowSize = 10 * 1000;
        if (options.speedMeasureConf) {
            if (options.speedMeasureConf.windowSizeMs)
                this.speedMeasureWindowSize = options.speedMeasureConf.windowSizeMs;

            if (options.speedMeasureConf.deltaMs)
                this.speedMeasureDeltaMs = options.speedMeasureConf.deltaMs;
        }
        this.perfMetrics = new ThroughputMeasurer({windowSizeMs: this.speedMeasureWindowSize});
        this._perfMetricTask = setInterval(() => {
            this.perfMetrics.measure(this.blocksSinceLastMeasure);
            this.blocksSinceLastMeasure = 0;
        }, this.speedMeasureDeltaMs);

        this.inputQueue = cargo(async (tasks) => {
            try {
                await this.processInputQueue(tasks);
            } catch (e) {
                this.logger.error(`Error on input queue worker: ${e}`);
                if (this.onError)
                    this.onError(e);
                else
                    process.exit(3);
            }
        }, this.options.maxMsgsInFlight);
    }


    private async processInputQueue(tasks: any[]) {
        for (const task of tasks) {
            await this.decodeShipData(task);
        }
        if (this.inputQueue.length() > this.options.maxMsgsInFlight) {
            this.inputQueue.pause();
            this.logger.info('Reader paused!');
        }
    }

    private resumeReading() {
        this.inputQueue.resume();
    }

    isActionRelevant(account: string, name: string): boolean {
        return (
            this.contracts.has(account) && (
                !this.options.actionWhitelist ||
                (this.options.actionWhitelist.has(account) &&
                 this.options.actionWhitelist.get(account).includes(name))
            )
        );
    }

    isDeltaRelevant(code: string, table: string): boolean {
        return (
            this.contracts.has(code) && (
                !this.options.tableWhitelist ||
                (this.options.tableWhitelist.has(code) &&
                 this.options.tableWhitelist.get(code).includes(table))
            )
        );
    }

    async start() {
        if (this.connecting)
            throw new Error('Reader already connecting');

        if (!this.stopped)
            throw new Error('Reader is not stopped, can\'t start!')

        this.logger.info(`Connecting to ${this.options.shipAPI}...`);
        this.connecting = true;

        this.ws = new WebSocket(this.options.shipAPI, {
            perMessageDeflate: false,
            maxPayload: this.options.maxPayloadMb * 1024 * 1024
        });

        await new Promise<void>((resolve, reject) => {
            this.ws.on('open', () => {
                this.connecting = false;
                this.stopped = false;
                if (this.onConnected)
                    this.onConnected();

                resolve();
            });
            this.ws.on('message', (msg: RawData) => {
                this.handleShipMessage(msg as Buffer);
            });
            this.ws.on('close', () => {
                this.connecting = false;
                this.shipAbiReady = false;
                if (this.onDisconnect)
                    this.onDisconnect();
                reject();
            });
            this.ws.on('error', (_: Error) => {
                this.connecting = false;
                this.shipAbiReady = false;
                reject();
            });
        });
    }

   stop() {
        this.logger.info('Stopping...');
        clearInterval(this._perfMetricTask);
        this.connecting = false;
        this.ws.close()
        this.shipAbiReady = false;
        this.stopped = true;
    }

    async restart(ms: number = 3000, blockNum: number) {
        if (this.restarting)
            throw new Error('Reader already restarting!');

        this.logger.info('Restarting...');
        this.restarting = true;
        this.connecting = false;
        this.ws.close()
        this.shipAbiReady = false;
        this.stopped = true;
        await new Promise<void>(resolve => {
            setTimeout(async () => {
                this.reconnectCount++;
                this.startBlock = blockNum;
                await this.start();
                this.restarting = false;
                resolve();
            }, ms);
        });
    }

    private send(param: (string | any)[]) {
        this.ws.send(Serializer.encode({
            type: 'request',
            object: param,
            abi: this.shipAbi
        }).array);
    }

    private ackBlockRange(size: number) {
        this.send(['get_blocks_ack_request_v0', {num_messages: size}]);
    }

    private handleShipMessage(msg: Buffer) {
        if (this.restarting) {
            this.logger.debug('dropping msg due to restart...');
            return;
        }
        if (this.stopped) {
            this.logger.debug('dropping msg due to stop...');
            return;
        }
        if (!this.shipAbiReady) {
            this.loadShipAbi(msg);
            return;
        }
        const result = Serializer.decode({type: 'result', abi: this.shipAbi, data: msg});
        switch (result[0]) {
            case 'get_blocks_result_v0': {
                try {
                    this.inputQueue.push(result[1], null);
                } catch (e) {
                    this.logger.error('[decodeShipData]', e.message);
                    this.logger.error(e);
                    if (this.onError)
                        this.onError(e);
                    else
                        process.exit(3);
                }
                break;
            }
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.logger.info(`Head block: ${data.head.block_num}`);
                const beginShipState = data.chain_state_begin_block;
                const endShipState = data.chain_state_end_block;
                if (this.startBlock < 0) {
                    this.startBlock = (this.options.irreversibleOnly ? data.last_irreversible.block_num : data.head.block_num) + this.startBlock;
                } else {
                    if (this.options.irreversibleOnly && this.startBlock > data.last_irreversible.block_num)
                        throw new Error(`irreversibleOnly true but startBlock > ship LIB`);
                }
                // only care about end state if end block < 0 or end block is max posible
                if (this.options.stopBlock < 0xffffffff - 1)
                    if (this.stopBlock < 0)
                        this.stopBlock = 0xffffffff - 1;
                    else if (this.stopBlock > endShipState)
                        throw new Error(`End block ${this.stopBlock} not in chain_state, end state: ${endShipState}`);

                if (this.startBlock <= beginShipState)
                    throw new Error(`Start block ${this.startBlock} not in chain_state, begin state: ${beginShipState} (must be +1 to startBlock)`);

                this.requestBlocks({
                    from: this.startBlock,
                    to: this.stopBlock
                });
                break;
            }
        }
    }

    private loadShipAbi(data: Buffer) {
        this.logger.info(`loading ship abi of size: ${data.length}`)
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ack();
    }

    private requestBlocks(param: { from: number; to: number }) {
        const from = param.from > 0 ? param.from : - 1;
        const to = param.to > 0 ? param.to + 1 : 0xffffffff;
        this.logger.info(`Requesting blocks from ${from} to ${to - 1}`);
        this.send(['get_blocks_request_v0', {
            start_block_num: from,
            end_block_num: to,
            max_messages_in_flight: this.options.maxMsgsInFlight,
            have_positions: [],
            irreversible_only: this.options.irreversibleOnly,
            fetch_block: this.options.fetchBlock,
            fetch_traces: this.options.fetchTraces,
            fetch_deltas: this.options.fetchDeltas,
        }]);
    }

    private async decodeShipData(resultElement: any) {
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        const blockNum = blockInfo.this_block.block_num;
        // const blockId = blockInfo.this_block.block_id;

        // const prevBlockNum = blockInfo.prev_block.block_num;
        // const prevBlockId = blockInfo.prev_block.block_id;

        // this.logger.debug('[decodeShipData]:');
        // this.logger.debug(`prev: #${prevBlockNum} - ${prevBlockId}`);
        // this.logger.debug(`this: #${blockNum} - ${blockId}`);

        if (blockNum > this.stopBlock) {
            this.logger.info(`dropping #${blockNum}, reached stop block...`);
            return;
        }

        let blockHeader = null;
        const decodedDeltas = [];
        const decodedActions = [];

        if (resultElement.block && blockNum) {

            const block = Serializer.decode({
                type: 'signed_block',
                data: resultElement.block.array as Uint8Array,
                abi: this.shipAbi
            }) as any;

            blockHeader = Serializer.objectify({
                timestamp: block.timestamp,
                producer: block.producer,
                confirmed: block.confirmed,
                previous: block.previous,
                transaction_mroot: block.transaction_mroot,
                action_mroot: block.action_mroot,
                schedule_version: block.schedule_version,
                new_producers: block.new_producers,
                header_extensions: block.header_extensions,
                producer_signature: block.producer_signature,
                block_extensions: block.block_extensions,
            });

            if (resultElement.deltas) {
                const deltaArrays = Serializer.decode({
                    type: 'table_delta[]',
                    data: resultElement.deltas.array as Uint8Array,
                    abi: this.shipAbi
                }) as any[];

                // process deltas
                for (let deltaArray of deltaArrays) {

                    // make sure the ABI for the watched contracts is updated before other processing is done
                    if (deltaArray[1].name === 'account') {
                        const abiRows = deltaArray[1].rows.map(r => {
                            if (r.present && r.data.array) {
                                const decodedRow = Serializer.decode({
                                    type: 'account',
                                    data: r.data.array,
                                    abi: this.shipAbi
                                });
                                if (decodedRow[1].abi) {
                                    return Serializer.objectify(decodedRow[1]);
                                }
                            }
                            return null;
                        }).filter(r => r !== null);
                        abiRows.forEach((abiRow) => {
                            if (this.contracts.has(abiRow.name)) {
                                this.logger.info(`new abi for ${abiRow.name} block_num: ${blockNum} ${abiRow.creation_date} abi hex len: ${abiRow.abi.length}`);
                                if (abiRow.abi.length == 0)
                                    return;
                                const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                                const abi = ABI.fromABI(new ABIDecoder(abiBin));
                                this.addContract(abiRow.name, abi);
                            }
                        });
                    }

                    if (deltaArray[1].name === 'contract_row') {
                        deltaArray[1].rows.forEach((row: any) => {
                            const deltaRow = Serializer.decode({
                                data: row.data.array,
                                type: 'contract_row',
                                abi: this.shipAbi
                            })[1];
                            const deltaObj = Serializer.objectify(deltaRow);
                            if (this.isDeltaRelevant(deltaObj.code, deltaObj.table)) {
                                const abi = this.contracts.get(deltaObj.code);
                                if (typeof abi === 'undefined') {
                                    throw new Error(`Cant decode delta for ${JSON.stringify(deltaObj, null, 4)}`);
                                }
                                const type = abi.tables.find(value => (value.name as Name).equals(deltaObj.table))?.type;
                                const dsValue = Serializer.decode({
                                    data: deltaObj.value,
                                    type, abi
                                });
                                const delta = Serializer.objectify(dsValue);
                                decodedDeltas.push(DeltaSchema.parse({
                                    code: deltaObj.code,
                                    table: deltaObj.table,
                                    delta
                                }));
                            }
                        });
                    }
                }
            }

            if (resultElement.traces) {
                const traces = Serializer.decode({
                    type: 'transaction_trace[]',
                    data: resultElement.traces.array as Uint8Array,
                    abi: this.shipAbi,
                    ignoreInvalidUTF8: true
                }) as any[];

                // process traces
                for (let trace of traces) {
                    const rt = Serializer.objectify(trace[1]);
                    if (!rt.partial || rt.partial.length < 2)
                        continue;

                    const partialTransaction = rt.partial[1];

                    for (const at of rt.action_traces) {
                        const actionTrace = at[1];
                        if (actionTrace.receipt === null) {
                            this.logger.warn(`action trace with receipt null! maybe hard_fail'ed deferred tx? block: ${blockNum}`);
                            continue;
                        }
                        if (this.isActionRelevant(actionTrace.act.account, actionTrace.act.name))  {
                            const abiActionNames = [];
                            this.contracts.get(actionTrace.act.account).actions.forEach((obj) => {
                                abiActionNames.push(obj.name.toString());
                            });
                            if (!abiActionNames.includes(actionTrace.act.name)) {
                                this.logger.warn(
                                    `action ${actionTrace.act.name} not found in ${actionTrace.act.account}'s abi, ignoring tx ${rt.id}...`);
                                continue;
                            }
                            const gs = actionTrace.receipt[1].global_sequence;
                            const action = actionTrace.act;
                            const abi = this.contracts.get(action.account);
                            const decodedActData = Serializer.decode({
                                data: action.data,
                                type: action.name,
                                ignoreInvalidUTF8: true,
                                abi
                            });
                            actionTrace.act.data = Serializer.objectify(decodedActData);
                            const extAction = {
                                global_sequence: gs,
                                action_ordinal: actionTrace.action_ordinal,
                                creator_action_ordinal: actionTrace.creator_action_ordinal,
                                trx_id: rt.id,
                                cpu: rt.cpu_usage_us,
                                net: rt.net_usage_words,
                                ram: actionTrace.account_ram_deltas,
                                receipt: actionTrace.receipt[1],
                                receiver: actionTrace.receiver,
                                console: actionTrace.console,
                                signatures: partialTransaction.signatures,
                                act: actionTrace.act
                            };
                            decodedActions.push(ActionSchema.parse(extAction));
                        }
                    }
                }
            }
        }

        const block = BlockSchema.parse({
            sequence: this.nextSequence++,
            status: blockInfo,
            header: blockHeader,
            deltas: decodedDeltas,
            actions: decodedActions
        });
        if (this.onBlock) {
            await this.onBlock(block);
        }

        this.blocksSinceLastMeasure++;

        if (blockNum == this.stopBlock) {
            if (this.onStop)
                this.onStop(block);
            return;
        }

        if (this.inputQueue.length() == 0)
            this.resumeReading();
    }

    addContract(account: string, abi: ABI) {
        if (account == 'eosio')
            addOnBlockToABI(abi);

        this.contracts.set(account, abi);
    }

    addContracts(contracts: {account: string, abi: ABI}[]) {
        for (const contract of contracts) {
            if (contract.account == 'eosio')
                addOnBlockToABI(contract.abi);

            this.contracts.set(contract.account, contract.abi);
        }
    }

    ack() {
        this.ackBlockRange(1);
    }

    get diagnostics() {
        return {
            start_block: this.startBlock,
            stop_block: this.stopBlock,
            reconnect_count: this.reconnectCount,
            stopped: this.stopped,
            connecting: this.connecting,
            restarting: this.restarting,
            ship_abi_ready: this.shipAbiReady,
            input_queue: {
                pending: this.inputQueue.length(),
                paused: this.inputQueue.paused
            },

        };
    }
}