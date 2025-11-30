# Token Accounting (Project: `ms_token_accounting`)

## Goal
The goal of this service is to keep the CIS tokens administration up to date. It does this by looking for new logged events (as created in [Events and Impacted Addresses](events_and_impacted.md)) and parses the events. 

## Data used
At every block (as an indicator, it's not actually looking at the block), this service reads the value of the helper `token_accounting_last_processed_block_v3`, which contains the last block we have processed that had events that led to a change for token accounting. 
It then tries to fetch any logged events from the `tokens_logged_events_v2` collection that have a `tx_info.block_height` larger than the retrieved value. 

Results are stored in the `tokens_links_v3` collection, that is a list of documents linking `accounts` to `tokens`. 

Token addresses are stored in the collection `tokens_token_addresses_v2`. 

!!! Note
    A `Token Address` is the combination of the `Contract Address` and the `token_id`. As `token_ids` are unique within a contract, the `Token Address` is globallyt unique. 

## Calculation steps
1. All logged events to be processed are retrieved from the `tokens_logged_events_v2` collection that have a `tx_info.block_height` larger than `token_accounting_last_processed_block_v3`. 
2. All events are grouped by `token_address`, as found in the event under `event_info.token_address`.

!!! Note
    For CIS-5 events involving CIS-2 contracts, the `token_address` needs to be created from the `recognized_event` properties.

3. Read stored information from these `token_addresses` as stored in the `tokens_token_addresses_v2` collection.
3. Looping through all events, if we see a new `token_address`, which has not yet been created/stored in the collection, we create a new `token_address`.
4. 
Depending on the `tag` from the `recognized_event`, we extract `addresses` that need to be linked to the `token_address`. 

!!! Note
    If it's an Account Address or Contract Address, we store a `canonical` version of the address, that is address[:29] (for contracts this is the same). This accounts for token holdings on aliases of accounts. 

    If it is a `public_key`, we need to store the key with reference to the wallet contract address, as public keys are only unique in the context of the contract addresss that has created the key. 


## Where is this data used?
These collections (`tokens_token_addresses_v2` `tokens_links_v3`) are used to display token holdings for an account, or displaying holders for a `token_address`.

## Examples
Public key holding a CIS-2 token:
```
{
  "_id": "<9635,0>-0e-<9632,0>-65c43f54ebca277747b7dc11078d2dc17286d8706905dca15a3ecd8f2fe61fbc",
  "account_address": "<9632,0>-65c43f54ebca277747b7dc11078d2dc17286d8706905dca15a3ecd8f2fe61fbc",
  "account_address_canonical": "<9632,0>-65c43f54ebca277747b7dc11078d2dc17286d8706905dca15a3ecd8f2fe61fbc",
  "token_holding": {
    "token_address": "<9635,0>-0e",
    "contract": "<9635,0>",
    "token_id": "0e"   
  }
}
```

Account holding a CIS-2 token:
```
{
  "_id": "<9390,0>--46Pu3wVfURgihzAXoDxMxWucyFo5irXvaEmacNgeK7i49MKyiD",
  "account_address": "46Pu3wVfURgihzAXoDxMxWucyFo5irXvaEmacNgeK7i49MKyiD",
  "account_address_canonical": "46Pu3wVfURgihzAXoDxMxWucyFo5i",
  "token_holding": {
    "token_address": "<9390,0>-",
    "contract": "<9390,0>",
    "token_id": ""
  }
}
```


Token address:
```
{
  "_id": "<9390,0>-",
  "contract": "<9390,0>",
  "token_id": "",
  "token_amount": "212032010000",
  "metadata_url": "https://euroeccdmetadataprod.blob.core.windows.net/euroeccdmetadataprod/euroe-concordium-offchain-data.json",
  "last_height_processed": 24801283,
  "exchange_rate": 1.037,
  "token_metadata": {
    "name": "EUROe Stablecoin",
    "symbol": "EUROe",
    "unique": false,
    "decimals": 6,
    "description": "EUROe is a modern European stablecoin - a digital representation of fiat Euros.",
    "thumbnail": {
      "url": "https://dev.euroe.com/persistent/token-icon/png/32x32.png"
    },
    "display": {
      "url": "https://dev.euroe.com/persistent/token-icon/png/256x256.png"
    },
    "artifact": {
      "url": "https://dev.euroe.com/persistent/token-icon/png/256x256.png"
    }
  },
  "hidden": false
}
```
