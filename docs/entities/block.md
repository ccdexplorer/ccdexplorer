The block is the fundamental object on the blockchain. Every 2s or so, a new block is created that may contain account transactions, update transactions or account creations. 

Blocks are stored in the collection `blocks` in MongoDB and represented as:


::: ccdexplorer.grpc_client.CCD_Types.CCD_BlockInfo
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 


Key augmentations:
- `transaction_hashes` – inserted by CCDExplorer so downstream jobs can jump from block metadata to transaction details without a secondary lookup.
- `finalized_time` and consensus info from `CCD_BlockInfo` to drive explorer charts such as TPS and block time stability.

When a finalized block arrives, we persist the [block](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockInfo), the [transactions](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockItemSummary) and the [special events](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockSpecialEvent).

## How is a block stored in the system?
1. [`heartbeat`](../projects/every_block/heartbeat.md) streams finalized blocks via gRPC and upserts them into MongoDB.
2. [`ms_block_analyser`](../projects/every_block/block_analyzer.md) consumes those inserts and emits Celery tasks so other services (indexers, events, metadata, etc.) can react per block.
3. Dagster jobs (e.g. [`dagster_nightrunner`](../projects/dagster_nightrunner.md)) read the `blocks` collection to build time-series such as block production per validator.***
