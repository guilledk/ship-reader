import WebSocket from "ws";
import {EventEmitter} from "events";
import fetch from "node-fetch";
import * as process from "process";
import * as console from "console";

import {addOnBlockToABI, logLevelToInt, ThroughputMeasurer} from "./utils.js";

import {ABI, ABIDecoder, APIClient, Serializer} from "@wharfkit/antelope";


export interface StateHistoryReaderOptions {
    shipApi: string;
    chainApi: string;
    irreversibleOnly?: boolean;
    blockHistorySize?: number;
    startBlock?: number;
    endBlock?: number;
    logLevel?: string;
    maxPayloadMb?: number;
    maxMsgsInFlight?: number;
    fetchBlock?: boolean;
    fetchTraces?: boolean;
    fetchDeltas?: boolean;
    actionWhitelist?: {[key: string]: string[]};  // key is code name, value is list of actions
    tableWhitelist?: {[key: string]: string[]};   // key is code name, value is list of tables,
    speedMeasureConf?: {
        windowSizeMs: number;
        deltaMs: number;
    };
}

export class StateHistoryReader {
    ws;
    maxPayloadMb: number;
    maxMsgsInFlight: number;
    reconnectCount = 0;
    private connecting = false;
    private shipAbi?: ABI;
    private shipAbiReady = false;
    events = new EventEmitter();

    private contracts: Map<string, ABI> = new Map();
    private actionWhitelist: Map<string, string[]>;
    private tableWhitelist: Map<string, string[]>;

    startBlock: number;
    endBlock: number;

    perfMetrics: ThroughputMeasurer;
    readonly speedMeasureWindowSize: number;
    readonly speedMeasureDeltaMs: number;

    private blocksSinceLastMeasure: number = 0;
    private _perfMetricTask;

    logLevel: string;

    api: APIClient;
    shipApi: string;
    private lastEmittedBlock: number;
    private irreversibleOnly: boolean;
    private fetchBlock: boolean;
    private fetchTraces: boolean;
    private fetchDeltas: boolean;

    onConnected: () => void = null;
    onDisconnect: () => void = null;
    onError: (err) => void = null;

    onBlock: (block) => void = null;

    constructor(private options: StateHistoryReaderOptions) {
        this.shipApi = options.shipApi;

        this.logLevel = options.logLevel ?? 'warning';
        this.maxPayloadMb = options.maxPayloadMb ?? 256;
        this.maxMsgsInFlight = options.maxMsgsInFlight ?? 1000;

        this.api = new APIClient({
            url: options.chainApi,
            fetch
        });

        this.irreversibleOnly = options.irreversibleOnly ?? false;
        this.fetchBlock = options.fetchBlock ?? true;
        this.fetchDeltas = options.fetchDeltas ?? true;
        this.fetchTraces = options.fetchTraces ?? true;

        this.startBlock = options.startBlock ?? -1;
        this.lastEmittedBlock = this.startBlock - 1;
        this.endBlock = options.endBlock ?? -1;

        if (options.actionWhitelist)
            this.actionWhitelist = new Map(Object.entries(options.actionWhitelist));

        if (options.tableWhitelist)
            this.tableWhitelist = new Map(Object.entries(options.tableWhitelist));

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
    }

    isActionRelevant(account: string, name: string): boolean {
        return (
            this.contracts.has(account) && (
                !this.actionWhitelist ||
                (this.actionWhitelist.has(account) &&
                 this.actionWhitelist.get(account).includes(name))
            )
        );
    }

    isDeltaRelevant(code: string, table: string): boolean {
        return (
            this.contracts.has(code) && (
                !this.tableWhitelist ||
                (this.tableWhitelist.has(code) &&
                 this.tableWhitelist.get(code).includes(table))
            )
        );
    }

    log(level: string, message?: any, ...optionalParams: any[]): void {
        if (logLevelToInt(this.logLevel) >= logLevelToInt(level))
            console.log(`[${(new Date()).toISOString().slice(0, -1)}][READER][${level.toUpperCase()}]`, message, ...optionalParams);
    }

    start() {
        if (this.connecting)
            throw new Error('Reader already connecting');

        this.log('info', 'Node range check done!');
        this.log('info', `Connecting to ${this.shipApi}...`);
        this.connecting = true;

        this.ws = new WebSocket(this.shipApi, {
            perMessageDeflate: false,
            maxPayload: this.maxPayloadMb * 1024 * 1024,
        });
        this.ws.on('open', () => {
            this.connecting = false;
            if (this.onConnected)
                this.onConnected();
        });
        this.ws.on('message', (msg) => {
            this.handleShipMessage(msg);
        });
        this.ws.on('close', () => {
            this.connecting = false;
            this.shipAbiReady = false;
            if (this.onDisconnect)
                this.onDisconnect();
        });
        this.ws.on('error', (err) => {
            this.connecting = false;
            this.shipAbiReady = false;
        });
    }

   stop() {
        this.log('info', 'Stopping...');
        clearInterval(this._perfMetricTask);
        this.connecting = false;
        this.ws.close()
        this.shipAbiReady = false;
    }

    restart(ms: number = 3000, forceBlock?: number) {
        this.log('info', 'Restarting...');
        this.connecting = false;
        this.ws.close()
        this.shipAbiReady = false;
        const restartBlock = forceBlock ? forceBlock : this.lastEmittedBlock + 1;
        setTimeout(() => {
            this.reconnectCount++;
            this.startBlock = restartBlock;
            this.start();
        }, ms);
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
        if (!this.shipAbiReady) {
            this.loadShipAbi(msg);
            return;
        }
        const result = Serializer.decode({type: 'result', abi: this.shipAbi, data: msg});
        switch (result[0]) {
            case 'get_blocks_result_v0': {
                try {
                    this.decodeShipData(result[1]);
                } catch (e) {
                    this.log('error', '[decodeShipData]', e.message);
                    this.log('error', e);
                }
                break;
            }
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.log('info', `Head block: ${data.head.block_num}`);
                const beginShipState = data.chain_state_begin_block;
                const endShipState = data.chain_state_end_block;
                if (this.startBlock < 0) {
                    this.startBlock = (this.irreversibleOnly ? data.last_irreversible.block_num : data.head.block_num) + this.startBlock;
                } else {
                    if (this.irreversibleOnly && this.startBlock > data.last_irreversible.block_num)
                        throw new Error(`irreversibleOnly true but startBlock > ship LIB`);
                }
                // only care about end state if end block < 0 or end block is max posible
                if (this.endBlock != 0xffffffff - 1)
                    if (this.endBlock < 0)
                        this.endBlock = 0xffffffff - 1;
                    else if (this.endBlock > endShipState)
                        throw new Error(`End block ${this.endBlock} not in chain_state, end state: ${endShipState}`);

                if (this.startBlock <= beginShipState)
                    throw new Error(`Start block ${this.startBlock} not in chain_state, begin state: ${beginShipState} (must be +1 to startBlock)`);

                this.lastEmittedBlock = this.startBlock - 1;
                this.requestBlocks({
                    from: this.startBlock,
                    to: this.endBlock
                });
                break;
            }
        }
    }

    private loadShipAbi(data: Buffer) {
        this.log('info', `loading ship abi of size: ${data.length}`)
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ackBlockRange(1);
    }

    private requestBlocks(param: { from: number; to: number }) {
        this.log('info', `Requesting blocks from ${param.from} to ${param.to}`);
        this.send(['get_blocks_request_v0', {
            start_block_num: param.from > 0 ? param.from : -1,
            end_block_num: param.to > 0 ? param.to + 1 : 0xffffffff,
            max_messages_in_flight: this.maxMsgsInFlight,
            have_positions: [],
            irreversible_only: this.irreversibleOnly,
            fetch_block: this.fetchBlock,
            fetch_traces: this.fetchTraces,
            fetch_deltas: this.fetchDeltas,
        }]);
    }

    private decodeShipData(resultElement: any) {
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        const prevBlockNum = blockInfo.prev_block.block_num;
        const prevBlockId = blockInfo.prev_block.block_id;

        const blockNum = blockInfo.this_block.block_num;
        const blockId = blockInfo.this_block.block_id;

        this.log('debug', '[decodeShipData]:');
        this.log('debug', `prev: #${prevBlockNum} - ${prevBlockId}`);
        this.log('debug', `this: #${blockNum} - ${blockId}`);

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
                                this.log('info', abiRow.name, `block_num: ${blockNum}`, abiRow.creation_date, `abi hex len: ${abiRow.abi.length}`);
                                if (abiRow.abi.length == 0)
                                    return;
                                console.time(`abiDecoding-${abiRow.name}-${blockNum}`);
                                const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                                const abi = ABI.fromABI(new ABIDecoder(abiBin));
                                console.timeEnd(`abiDecoding-${abiRow.name}-${blockNum}`);
                                this.addContract(abiRow.name, abi);
                            }
                        });
                    }


                    if (deltaArray[1].name === 'contract_row') {
                        deltaArray[1].rows.forEach((row: any, index: number) => {
                            const deltaRow = Serializer.decode({
                                data: row.data.array,
                                type: 'contract_row',
                                abi: this.shipAbi
                            })[1];
                            const deltaObj = Serializer.objectify(deltaRow);
                            if (this.isDeltaRelevant(deltaObj.code, deltaObj.table)) {
                                const extDelta = {
                                    present: row.present,
                                    ...deltaObj
                                };
                                const abi = this.contracts.get(deltaObj.table);
                                const type = abi.tables.find(value => value.name === deltaObj.table)?.type;
                                const dsValue = Serializer.decode({
                                    data: deltaObj.value,
                                    type, abi
                                });
                                decodedDeltas.push(Serializer.objectify(dsValue));
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
                            this.log('warning', `action trace with receipt null! maybe hard_fail'ed deferred tx? block: ${blockNum}`);
                            continue;
                        }
                        if (this.isActionRelevant(actionTrace.act.account, actionTrace.act.name))  {
                            const abiActionNames = [];
                            this.contracts.get(actionTrace.act.account).actions.forEach((obj) => {
                                abiActionNames.push(obj.name.toString());
                            });
                            if (!abiActionNames.includes(actionTrace.act.name)) {
                                this.log(
                                    'warning',
                                    `action ${actionTrace.act.name} not found in ${actionTrace.act.account}'s abi, ignoring tx ${rt.id}...`);
                                continue;
                            }
                            const gs = actionTrace.receipt[1].global_sequence;
                            const extAction = {
                                actionOrdinal: actionTrace.action_ordinal,
                                creatorActionOrdinal: actionTrace.creator_action_ordinal,
                                trxId: rt.id,
                                cpu: rt.cpu_usage_us,
                                net: rt.net_usage_words,
                                ram: actionTrace.account_ram_deltas,
                                receipt: actionTrace.receipt[1],
                                receiver: actionTrace.receiver,
                                console: actionTrace.console,
                                signatures: partialTransaction.signatures,
                                act: actionTrace.act
                            };
                            const action = actionTrace.act;
                            const abi = this.contracts.get(action.account);
                            const decodedActData = Serializer.decode({
                                data: action.data,
                                type: action.name,
                                ignoreInvalidUTF8: true,
                                abi
                            });
                            decodedActions.push(Serializer.objectify(decodedActData));
                        }
                    }
                }
            }
        }

        if (this.onBlock) {
            this.onBlock({
                blockInfo,
                blockHeader,
                deltas: decodedDeltas,
                actions: decodedActions,
                createdAt: process.hrtime.bigint()
            });
        }

        this.blocksSinceLastMeasure++;
        this.lastEmittedBlock = blockNum;
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

    ack(amount?: number) {
        if (typeof amount === undefined)
            amount = 1;

        this.ackBlockRange(amount);
    }
}