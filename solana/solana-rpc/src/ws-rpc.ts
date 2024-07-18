import {RpcClient, SubscriptionHandle} from '@subsquid/rpc-client'
import {GetBlock} from '@subsquid/solana-rpc-data'
import {ANY_OBJECT, assertValidity, GetSrcType, NAT, nullable, object} from '@subsquid/util-internal-validation'
import {Commitment, DataRequest} from './base'
import {GetBlockOptions} from './rpc'
import assert from 'assert'

export interface BlockSubscribeOptions extends GetBlockOptions {
    commitment: Commitment
    onBlockNotification: (msg: BlockNotification) => void
    onError: (err: Error) => void
}

export class WsRpc {
    constructor(private client: RpcClient) {
        assert(this.client.supportsNotifications())
    }

    blockSubscribe(options: BlockSubscribeOptions): SubscriptionHandle {
        return this.client.subscribe({
            method: 'blockSubscribe',
            params: [
                'all',
                {
                    commitment: options.commitment,
                    showRewards: options.rewards,
                    transactionDetails: options.transactionDetails,
                    maxSupportedTransactionVersion: options.maxSupportedTransactionVersion,
                    encoding: 'json',
                },
            ],
            unsubscribe: 'blockUnsubscribe',
            notification: 'blockNotification',
            onMessage: (msg) => {
                try {
                    assertValidity(BlockNotification, msg)
                } catch (e: any) {
                    return options.onError(e)
                }

                options.onBlockNotification(msg)
            },
            onError: options.onError,
            resubscribeOnConnectionLoss: true,
        })
    }
}

export const BlockNotification = object({
    value: object({
        slot: NAT,
        block: nullable(GetBlock),
        err: nullable(ANY_OBJECT),
    }),
})

export type BlockNotification = GetSrcType<typeof BlockNotification>
