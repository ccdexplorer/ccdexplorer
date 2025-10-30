Native accounts on Concordium are created through an [Account Creation](../entities/transaction.md#account-creation) transaction. 

::: ccdexplorer.grpc_client.CCD_Types.CCD_AccountInfo
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 
      show_root_heading: true


## How is an account stored in the system?
Technically, we do not store the account with all info in the db itself, only in the collection `all_account_addresses` a new entry is stored. This is performed in the [New Address Service](../projects/every_block/new_address.md), that works on receiving the appropriate MQTT message. 
