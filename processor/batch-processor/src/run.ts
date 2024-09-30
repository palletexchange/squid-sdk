import {createLogger} from '@subsquid/logger'
import {last, maybeLast, runProgram, Throttler} from '@subsquid/util-internal'
import {createPrometheusServer} from '@subsquid/util-internal-prometheus-server'
import * as prom from 'prom-client'
import {Database, HashAndHeight, HotDatabaseState, HotTxInfo} from './database'
import {DataSource} from './datasource'
import {Metrics} from './metrics'
import {formatHead, getItemsCount} from './util'
import assert from 'assert'
import {AlreadyIndexedBlockNotFoundError, FinalizedHeadBelowStateError} from './errors'


const log = createLogger('sqd:batch-processor')


export interface DataHandlerContext<Block, Store> {
    /**
     * Storage interface provided by the database
     */
    store: Store
    /**
     * List of blocks to map and process
     */
    blocks: Block[]
    /**
     * Signals, that the processor is near the head of the chain.
     */
    isHead: boolean
}


export interface BlockBase {
    header: HashAndHeight
}


/**
 * Run data processing.
 *
 * This method assumes full control over the current OS process as
 * it terminates the entire program in case of error or
 * at the end of data processing.
 *
 * @param src - data source to ingest data from
 *
 * @param db - database is responsible for providing storage API to data handler
 * and persisting mapping progress and status.
 *
 * @param dataHandler - The data handler, see {@link DataHandlerContext} for an API available to the handler.
 */
export function run<Block extends BlockBase, Store>(
    src: DataSource<Block>,
    db: Database<Store>,
    dataHandler: (ctx: DataHandlerContext<Block, Store>) => Promise<void>
): void {
    runProgram(
        () => {
            return new Processor(src, db, dataHandler).run()
        },
        (err) => {
            log.fatal(err)
        }
    )
}


export class Processor<B extends BlockBase, S> {
    private metrics = new Metrics()
    private chainHeight: Throttler<number>
    private statusReportTimer?: any
    private hasStatusNews = false

    /**
     * Constructs a new instance of the Processor class.
     *
     * @param src - data source to ingest data from
     * 
     * @param db - database is responsible for providing storage API to data handler
     * and persisting mapping progress and status
     * 
     * @param dataHandler - The data handler, see {@link DataHandlerContext} for an API available to the handler.
     */
    constructor(
        private src: DataSource<B>,
        private db: Database<S>,
        private handler: (ctx: DataHandlerContext<B, S>) => Promise<void>
    ) {
        this.chainHeight = new Throttler(() => this.src.getFinalizedHeight(), 30_000)
    }

    async run(): Promise<void> {
        let state = await this.getDatabaseState()

        // remove all hot block to start from the finalized head
        state = {...state, top: []}

        if (state.height >= 0) {
            log.info(`last processed final block was ${state.height}`)
        }

        await this.assertWeAreOnTheSameChain(state)
        await this.initMetrics(state)

        for await (let {blocks, finalizedHead} of this.src.getBlockStream({
            range: {from: state.height + 1},
            supportHotBlocks: this.db.supportsHotBlocks,
        })) {
            let mappingStartTime = process.hrtime.bigint()

            state = await this.processBatch(state, finalizedHead, blocks)

            let mappingEndTime = process.hrtime.bigint()

            await this.updateProgressMetrics(maybeLast(state.top) || state, mappingEndTime)
            this.metrics.registerBatch(blocks.length, getItemsCount(blocks), mappingStartTime, mappingEndTime)
    
            this.reportStatus()
        }

        this.reportFinalStatus()
    }

    private async assertWeAreOnTheSameChain(state: HashAndHeight): Promise<void> {
        if (state.height < 0) return
        let hash = await this.src.getBlockHash(state.height)
        if (state.hash === hash) return
        throw new AlreadyIndexedBlockNotFoundError(state)
    }

    private async initMetrics(state: HashAndHeight): Promise<void> {
        await this.updateProgressMetrics(state)
        let port = process.env.PROCESSOR_PROMETHEUS_PORT || process.env.PROMETHEUS_PORT
        if (port == null) return
        prom.collectDefaultMetrics()
        this.metrics.install()
        let server = await createPrometheusServer(prom.register, port)
        log.info(`prometheus metrics are served on port ${server.port}`)
    }

    private async updateProgressMetrics(state: HashAndHeight, time?: bigint): Promise<void> {
        let chainHeight = await this.chainHeight.get()

        this.metrics.setChainHeight(chainHeight)
        this.metrics.setLastProcessedBlock(state.height)
        let left: number
        let processed: number
        if (this.src.getBlocksCountInRange) {
            left = this.src.getBlocksCountInRange({
                from: this.metrics.getLastProcessedBlock() + 1,
                to: this.metrics.getChainHeight(),
            })
            processed =
                this.src.getBlocksCountInRange({
                    from: 0,
                    to: this.metrics.getChainHeight(),
                }) - left
        } else {
            left = this.metrics.getChainHeight() - this.metrics.getLastProcessedBlock()
            processed = this.metrics.getLastProcessedBlock()
        }
        this.metrics.updateProgress(processed, left, time)
    }

    private async processBatch(prevState: HotDatabaseState, finalizedHead: HashAndHeight, blocks: B[]): Promise<HotDatabaseState> {
        if (prevState.height > finalizedHead.height) {
            throw new FinalizedHeadBelowStateError(finalizedHead, prevState)
        }

        if (prevState.height === finalizedHead.height && prevState.hash !== finalizedHead.hash) {
            throw new Error()
        }

        if (prevState.height > blocks[0].header.height) {
            throw new Error()
        }

        if (prevState.height === blocks[0].header.height && prevState.hash !== blocks[0].header.hash) {
            throw new Error()
        }

        let lastBlock = last(blocks).header
        let nextState: HotDatabaseState =
            lastBlock.height < finalizedHead.height
                ? toDatabaseState(lastBlock)
                : toDatabaseState(finalizedHead)
        
        let baseHead = prevState as HashAndHeight
        for (let block of prevState.top) {
            if (block.height > blocks[0].header.height) break
            if (block.height === blocks[0].header.height && block.hash !== blocks[0].header.hash) break
            baseHead = block
            if (block.height <= nextState.height) continue
            nextState.top.push(block)
        }

        for (let {header: block} of blocks) {
            if (block.height <= finalizedHead.height) continue
            nextState.top.push(block)
        }

        let prevHead = maybeLast(prevState.top) || prevState
        let nextHead = maybeLast(nextState.top) || nextState

        if (baseHead.hash !== prevHead.hash) {
            log.info(`navigating a fork between ${formatHead(prevHead)} to ${formatHead(nextHead)} with a common base ${formatHead(baseHead)}`)
        }

        if (nextHead.height === nextState.height && prevHead.height === prevState.height) {
            await this.db.transact(
                {
                    prevHead,
                    nextHead,
                },
                (store) => {
                    return this.handler({
                        store,
                        blocks,
                        isHead: nextHead.height === finalizedHead.height,
                    })
                }
            )
        } else {
            assert(this.db.supportsHotBlocks, 'database does not support hot blocks')

            let info: HotTxInfo = {
                finalizedHead,
                baseHead,
                newBlocks: blocks.map(b => b.header),
            }
            if (this.db.transactHot2) {
                await this.db.transactHot2(
                    info,
                    (store, blockSliceStart, blockSliceEnd) => {
                        return this.handler({
                            store,
                            blocks: blocks.slice(blockSliceStart, blockSliceEnd),
                            isHead: blockSliceEnd === blocks.length,
                        })
                    }
                )
            } else {
                await this.db.transactHot(info, (store, ref) => {
                    let idx = ref.height - prevState.height - 1
                    let block = blocks[idx]

                    assert.strictEqual(block.header.hash, ref.hash)
                    assert.strictEqual(block.header.height, ref.height)

                    return this.handler({
                        store,
                        blocks: [block],
                        isHead: nextHead.height === ref.height,
                    })
                })
            }
        }

        return nextState
    }
    
    private reportStatus(): void {
        if (this.statusReportTimer == null) {
            log.info(this.metrics.getStatusLine())
            this.statusReportTimer = setTimeout(() => {
                this.statusReportTimer = undefined
                if (this.hasStatusNews) {
                    this.hasStatusNews = false
                    this.reportStatus()
                }
            }, 5000)
        } else {
            this.hasStatusNews = true
        }
    }

    private reportFinalStatus(): void {
        if (this.statusReportTimer != null) {
            clearTimeout(this.statusReportTimer)
        }
        if (this.hasStatusNews) {
            this.hasStatusNews = false
            log.info(this.metrics.getStatusLine())
        }
    }

    private async getDatabaseState(): Promise<HotDatabaseState> {
        if (this.db.supportsHotBlocks) {
            return await this.db.connect()
        } else {
            return await this.db.connect().then(head => {
                return {...head, top: []}
            })
        }
    }
}

function toDatabaseState(block: HashAndHeight): HotDatabaseState {
    return {
        height: block.height,
        hash: block.hash,
        top: [],
    }
}