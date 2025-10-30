The heartbeat service is the `heartbeat` of CCDExplorer Universe. It watches for new finalized blocks and parses them. 
It's main responsibility is watch the node and on every new block, save the block and transactions in the Mongo database. 

It's run per net, so deployed as a `mainnet` and `testnet` service. 

## Collections impacted
- `blocks`: every block is stored in this collection as a [CCD_BlockInfo](../../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockInfo) type.
- `transactions`: every transaction is stored in this collection as a [CCD_BlockItemSummary](../../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockItemSummary) type. Note that the services tries to classify transcaction by adding a `recognized_sender_id` if the [CCD_AccountTransactionDetails](../../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_AccountTransactionDetails) `sender` is found in collection `projects`.
