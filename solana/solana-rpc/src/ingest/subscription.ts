import {SubscriptionHandle} from '@subsquid/rpc-client'
import {Block} from '@subsquid/solana-rpc-data'
import {addErrorContext, assertNotNull, AsyncQueue, ensureError, last, maybeLast} from '@subsquid/util-internal'
import assert from 'assert'
import {Commitment, DataRequest} from '../base'
import {BlockNotification, WsRpc} from '../ws-rpc'

export class SubscriptionStream {
    private blocks: Block[] = []
    private notify = new AsyncQueue<null | Error>(1)
    private handle?: SubscriptionHandle
    private pos: number

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

    getHeadSlot(): number {
        if (this.blocks.length > 0) {
            let head = last(this.blocks)
            return head.slot
        } else {
            return this.pos
        }
    }

    async *getBlocks(): AsyncIterable<Block[]> {
        assert(this.handle == null, 'subscription is already in use')
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
        for await (let err of this.notify.iterate()) {
            if (err instanceof Error) throw err

            let blocks = this.blocks
            if (blocks.length > 0) {
                this.pos = last(blocks).slot
            }
            this.blocks = []

            yield blocks
        }
    }

    private error(err: unknown): void {
        this.notify.forcePut(ensureError(err))
        this.notify.close()
    }

    private onBlockNotification(msg: BlockNotification): void {
        let {slot, err, block} = msg.value

        if (slot < this.pos) return

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
        while (this.blocks.length && last(this.blocks).height >= block.height) {
            this.blocks.pop()
        }
        if (maybeLast(this.blocks)?.height === block.height - 1) {
            if (last(this.blocks).hash !== block.block.previousBlockhash) {
                this.blocks.pop()
            }
        }

        this.blocks.push(block)
        this.notify.forcePut(null)

        if (this.blocks.length >= 50) {
        }
    }
}
