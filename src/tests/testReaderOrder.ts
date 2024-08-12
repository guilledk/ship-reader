import {StateHistoryReader} from "../reader.js";
import {ABI} from "@wharfkit/antelope";
import {readFileSync} from "node:fs";
import {expect} from 'chai';
import * as console from "console";
import {Block, StateHistoryReaderOptionsSchema} from "../types.js";
import * as process from "node:process";

const options = StateHistoryReaderOptionsSchema.parse({
    shipAPI: 'ws://127.0.0.1:29999',
    chainAPI: 'http://127.0.0.1:8888',
    startBlock: 180698860,
    stopBlock:  180700000,
    actionWhitelist: {
        'eosio.token': ['transfer'],
        'eosio.evm': ['raw', 'withdraw']
    },
    tableWhitelist: {
        'eosio.evm': ['config', 'account', 'accountstate']
    },
    logLevel: 'info',
    maxPayloadMb: Math.floor(1024 * 1.5)
});

const reader = new StateHistoryReader(options);
reader.onError = (err) => {throw err};

const abis = ['eosio', 'eosio.evm', 'eosio.token'].map((abiFileNames) => {
    const jsonAbi = JSON.parse(readFileSync(`./${abiFileNames}.abi`).toString())
    return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
});

reader.addContracts(abis);

let lastPushed = options.startBlock - 1;
let lastPushedTS = 'unknown';
let totalRead = 0;
let firstBlock = -1;
let firstBlockTs: number;

const statsTask = setInterval(() => {
    if (lastPushed >= options.startBlock && reader.perfMetrics.max > 0) {
        const avgSpeed = reader.perfMetrics.average.toFixed(2).padStart(8, ' ');
        const maxSpeed = reader.perfMetrics.max.toFixed(2).padStart(8, ' ')
        console.log(`${lastPushed} @ ${lastPushedTS}: avg speed: ${avgSpeed} blocks/s | max speed: ${maxSpeed} blocks/s`);
    }
}, 1000);

const onBlock = async (block: Block) => {
    const currentBlock = block.status.this_block.block_num;

    if (firstBlock < 0) {
        firstBlock = currentBlock;
        firstBlockTs = performance.now();
    }

    expect(currentBlock).to.be.equal(lastPushed + 1);
    lastPushed = block.status.this_block.block_num;
    lastPushedTS = block.header.timestamp;
    totalRead++;
    reader.ack();
};

reader.onBlock = onBlock;

reader.onStop = (lastBlock: Block) => {
    const elapsedMs = performance.now() - firstBlockTs;
    const elapsedS = elapsedMs / 1000;
    console.log(`elapsed sec: ${elapsedS}`);
    console.log(`avg speed: ${(totalRead / elapsedS).toFixed(2)}`);

    if (options.startBlock > 0)
        expect(firstBlock, 'First block received mismatch!').to.be.equal(options.startBlock);

    if (options.stopBlock > 0)
        expect(lastPushed, 'Last block received mismatch!').to.be.equal(options.stopBlock);

    expect(lastPushed, 'Last block on stop event doesnt match last received!').to.be.equal(lastBlock.status.this_block.block_num);

    reader.stop();

    clearInterval(statsTask);
    process.exit(0);
};

await reader.start();