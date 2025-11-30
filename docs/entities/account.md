Native accounts on Concordium are created through an [Account Creation](../entities/transaction.md#account-creation) transaction. 

::: ccdexplorer.grpc_client.CCD_Types.CCD_AccountInfo
    handler: python
    options:
      show_source: true
      show_bases: false
      show_signature: true 
      show_root_heading: true


## Canonical vs alias
Accounts can expose multiple aliases (derived sub-addresses). CCDExplorer stores:
- `account_address` – the full alias that appeared on-chain.
- `account_address_canonical` – the first 29 characters shared by every alias of the account.  
See [Alias Addresses](alias.md) for details on how we normalize aliases across services.

## How is an account stored in the system?
- [`ms_accounts`](../projects/every_block/new_address.md) listens for the `account_creation` Celery topic and inserts a document into `all_account_addresses` with both the canonical ID and the newest alias.
- The actual account metadata (balances, delegation, credentials) is retrieved on demand via `GetAccountInfo`.  
  Long-running analytics snapshots (e.g. [`accounts_retrieval`](../projects/accounts_retrieval.md)) persist denormalized copies for historical analysis.
- Helper collections (statistics, account graphs, notification preferences) use the canonical ID to ensure aliases roll up correctly.***
