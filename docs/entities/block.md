The block is the fundamental object on the blockchain. Every 2s or so, a new block is created that may contain account transactions, update transactions or account creations. 

Blocks are stored in the collection `blocks` in the db and has the following class:


::: ccdexplorer.grpc_client.CCD_Types.CCD_BlockInfo
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 


Note that we have added the property `transaction_hashes` to the block class, to enable indexing transactions and blocks together. 
Finally, when a finalized block arrives, we store the [block](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockInfo), the [transactions](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockItemSummary) and the [special events](../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_BlockSpecialEvent).

## How is a block stored in the system?
In the [heartbeat](../projects/every_block/heartbeat.md) service, all new finalized blocks are parsed and stored in the db.