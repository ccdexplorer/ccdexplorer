
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

Note that for account transactions, we try to add a `recognized_sender_id` to each transaction. This value is retrieved from `projects.json`. If a match is made, the `project_id` is stored as `recognized_sender_id`.

## Update
Update transactions are performed by the chain itself. The following payloads are possible:
::: ccdexplorer.grpc_client.CCD_Types.CCD_UpdatePayload
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 



## How is a transaction stored in the system?
In the [heartbeat](../services/every_block/heartbeat.md) service, all new transactions are parsed and stored in the db.