import {SubscriptionHandle} from '@subsquid/rpc-client'
import {Block} from '@subsquid/solana-rpc-data'
import {addErrorContext, assertNotNull, AsyncQueue, createFuture, ensureError, Future, last, maybeLast} from '@subsquid/util-internal'
import assert from 'assert'
import {Commitment, DataRequest} from '../base'
import {BlockNotification, WsRpc} from '../ws-rpc'
import {addTimeout, TimeoutError} from '@subsquid/util-timeout'

export class SubscriptionStream {
    private blocks: Block[] = []
    private notify = new AsyncQueue<null | Error>(1)
    private handle?: SubscriptionHandle
    private getFirstSlotFuture?: Future<number>
    private pos: number
    private lastHeight?: number

    constructor(
        private rpc: WsRpc,
        private newHeadTimeout: number,
        private commitment: Commitment,
        private req: DataRequest,
        fromSlot: number
    ) {
        this.notify.addCloseListener(() => {
            this.handle?.close()
            this.handle = undefined
        })
        this.pos = fromSlot - 1
    }

    async getFirstSlot(): Promise<number> {
        if (this.blocks.length > 0) {
            return this.blocks[0].slot
        } else {
            this.subscribe()

            this.getFirstSlotFuture = this.getFirstSlotFuture || createFuture()
            return this.getFirstSlotFuture.promise()
        }
    }

    async *getBlocks(): AsyncIterable<Block[]> {
        this.subscribe()
        
        while (true) {
            let err = await addTimeout(this.notify.take(), this.newHeadTimeout).catch(ensureError)

            if (err instanceof TimeoutError) {
                this.rpc.client.reset()
            } else if (err instanceof Error) {
                throw err
            } else {
                if (this.blocks.length > 0) {
                    let blocks = this.blocks
                    if (blocks.length > 0) {
                        this.pos = last(blocks).slot
                    }
                    this.blocks = []
        
                    yield blocks
                }
                
                if (err === undefined) return
            }
        }
    }

    private subscribe() {
        if (this.handle != null) return

        this.handle = this.rpc.blockSubscribe({
            commitment: this.commitment,
            rewards: this.req.rewards,
            transactionDetails: this.req.transactions ? 'full' : 'none',
            maxSupportedTransactionVersion: 0,
            onBlockNotification: (msg: BlockNotification) => {
                try {
                    this.onBlockNotification(msg)
                } catch (err: any) {
                    this.error(err)
                }
            },
            onError: (err) => this.error(err),
        })
    }

    private error(err: unknown): void {
        this.notify.forcePut(ensureError(err))
        this.notify.close()
    }

    private onBlockNotification(msg: BlockNotification): void {
        let {slot, err, block} = msg.value

        if (slot <= this.pos) return

        if (err) {
            throw addErrorContext(new Error(`Got a block notification error at slot ${slot}`), {
                blockNotificationError: err,
            })
        }

        if (block == null) return

        if (block.transactions == null && this.req.transactions) {
            throw new Error(`Transactions are missing in block notification at slot ${slot} although where requested`)
        }

        if (block.rewards == null && this.req.rewards) {
            throw new Error(`Rewards are missing in block notification at slot ${slot} although where requested`)
        }

        this.push({
            hash: block.blockhash,
            height: assertNotNull(block.blockHeight),
            slot,
            block,
        })
    }

    private push(block: Block): void {
        if (this.lastHeight != null && this.lastHeight + 1 < block.height) {
            return this.notify.close()
        }

        while (this.blocks.length && last(this.blocks).height >= block.height) {
            this.blocks.pop()
        }
        if (maybeLast(this.blocks)?.height === block.height - 1) {
            if (last(this.blocks).hash !== block.block.previousBlockhash) {
                this.blocks.pop()
            }
        }

        if (this.blocks.length < 50) {
            this.blocks.push(block)
            this.notify.forcePut(null)
    
            this.getFirstSlotFuture?.resolve(block.slot)
            this.getFirstSlotFuture = undefined
    
            this.lastHeight = block.height
        } else {
            this.notify.close()
        }
    }
}
