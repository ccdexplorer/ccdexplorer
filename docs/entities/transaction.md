
Transactions in the Concordium blockchain can be any of the following three:

1. [Account Creation](#account-creation)
2. [Account Transaction](#account-transaction)
3. [Update](#update)

## Account Creation
Whenever a user uses one of the various Concordium Wallets to create a new account, an `account_creation` transaction is added to a block. 


::: ccdexplorer.grpc_client.CCD_Types.CCD_AccountCreationDetails
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 

## Account Transaction
These are user initiated transactions and paid for by the sender. Account transactions can have the following types:

::: ccdexplorer.grpc_client.CCD_Types.CCD_AccountTransactionEffects
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 

Note that for account transactions, we add a `recognized_sender_id` whenever the sender or invoked contract matches an entry in `projects.json`.  
The lookup happens inside [`ms_indexers`](../projects/every_block/indexers.md) so every document in the `transactions` collection carries a `project_id` tag when possible.

## Update
Update transactions are performed by the chain itself. The following payloads are possible:
::: ccdexplorer.grpc_client.CCD_Types.CCD_UpdatePayload
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 



## Augmentations performed by CCDExplorer
- [`ms_events_and_impacts`](../projects/every_block/events_and_impacted.md) adds `impacted_addresses`, including canonical address references for alias-safe queries.
- [`ms_indexers`](../projects/every_block/indexers.md) fans out transactions into specialized collections (`transactions_transfer`, `transactions_contracts`, etc.) for fast explorer lookups.
- [`ms_token_accounting`](../projects/every_block/token_accounting.md) reads `logged_events` emitted by `contract_update_issued` transactions to update token ledgers.

## How is a transaction stored in the system?
`heartbeat` ingests each block’s transactions and writes them to MongoDB; follow-up workers (listed above) enrich the base documents with project IDs, alias-friendly fields, and derived statistics.***
