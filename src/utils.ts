import {ABI} from "@wharfkit/antelope";

const onblockAbiAction: ABI.Action = {
    name: 'onblock',
    type: 'onblock',
    ricardian_contract: ''
};

const onblockAbiStruct: ABI.Struct = {
    name: 'onblock',
    base: '',
    fields: [
        { name: 'header', type: 'block_header' } // Placeholder for ignored type
    ]
};

export function addOnBlockToABI(abi: ABI) {
    abi.structs = [onblockAbiStruct, ...abi.structs];
    abi.actions = [onblockAbiAction, ...abi.actions];
}

export interface ThroughputStats {
    measures: number;
    elapsedMs?: number;
}

export class ThroughputMeasurer {
    private measures: {
        value: number;
        time: number;
    }[] = [];
    private maxValue: number = 0;
    private _startTime: number = undefined;

    // have at max windowSize millisecond old measures
    private readonly windowSizeMs: number;

   constructor(options: {
       windowSizeMs: number;
   }) {
       this.windowSizeMs = options.windowSizeMs;
   }

   measure(value: number) {
       const now = performance.now();
       this.measures.push({value, time: now});

       if (this._startTime == undefined)
           this._startTime = now;

       if (value > this.maxValue)
           this.maxValue = value;

       while(now - this.measures[0].time > this.windowSizeMs)
           this.measures.shift();
   }

   get startTime() {
       return this._startTime;
   }

   get average() {
       if (this.measures.length == 0) return 0;

       let acc = 0;
       for (const measure of this.measures)
           acc += measure.value;

       return acc / this.measures.length;
   }

   get max() {
       return this.maxValue;
   }

   get stats(): ThroughputStats  {
       const stats: ThroughputStats = {
           measures: this.measures.length
       };

       if (stats.measures > 0) {
           stats.elapsedMs = this.measures[stats.measures - 1].time - this._startTime;
       }
       return stats;
   }
}