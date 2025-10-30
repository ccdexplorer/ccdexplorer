
## Goal
The goal of this script is to calculate, on a daily basis, which transaction types have been used for all transactions. We calculate counts per day for all [Account Transaction Effects](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_AccountTransactionEffects), in addition to [Update Transaction Payloads](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_UpdatePayload) and [Account Creation Details](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_AccountCreationDetails).

## Data used
Data is calculated through pipelines, executed on the `transactions` collection. 

*Pipeline for types*:
```
pipeline = [
            {
                "$match": {
                    "block_info.height": {
                        "$gte": height_for_first_block,
                        "$lte": height_for_last_block,
                    },
                }
            },
            {"$sortByCount": "$type.type"}
        ]

```
The `type.type` field is filled with either `update`, `account_creation` or `account_transaction`. See [CCD_BlockItemSummary](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_BlockItemSummary).

*Pipeline for contents*:
```
pipeline = [
            {
                "$match": {
                    "block_info.height": {
                        "$gte": height_for_first_block,
                        "$lte": height_for_last_block,
                    },
                }
            },
            {"$sortByCount": "$type.contents"}
        ]

```


The `type.contents` field is filled with all possible values from [Account Transaction Effects](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_AccountTransactionEffects), in addition to [Update Transaction Payloads](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_UpdatePayload), [Account Creation Details](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_AccountCreationDetails) and [Reject Reasons](../../components/grpc.md#ccdexplorer.grpc_client.CCD_Types.CCD_RejectReason).

## Examples
This is an example for 2025-02-21:
```
{
  "date": "2025-02-21",
  "data_registered": 490,
  "contract_update_issued": 469,
  "account_transfer": 107,
  "micro_ccd_per_euro_update": 48,
  "normal": 10,
  "delegation_configured": 9,
  "amount_too_large": 1,
  "rejected_receive": 1,
  "insufficient_balance_for_delegation_stake": 1,
  "account_transaction": 1078,
  "update": 48,
  "account_creation": 10
}
```

And another example for the first and second days of the chain:
```
{
  "date": "2021-06-09",
  "initial": 92,
  "normal": 16,
  "account_transfer": 15,
  "transferred_to_public": 3,
  "baker_removed": 2,
  "baker_added": 2,
  "encrypted_amount_transferred": 2,
  "transferred_to_encrypted": 1,
  "transferred_with_schedule": 1,
  "account_creation": 108,
  "account_transaction": 26
}
```
```
{
  "date": "2021-06-10",
  "initial": 171,
  "normal": 27,
  "account_creation": 198
}
```
!!! note 
    `initial` and `normal` are two possible ways accounts are created, depending on the wallet type. Both are counted as `account_creation`.

## Where is this data used?

1. We publish a [chart](https://ccdexplorer.io/mainnet/charts/transactions-count) that uses this data. Note that the chart only displays a subset. 

| Grouping | Keys |
|----------|------|
| Account | `account_creation`<br>`credential_keys_updated`<br>`credentials_updated` |
| Staking | `baker_configured`<br>`baker_added`<br>`baker_removed`<br>`baker_keys_updated`<br>`baker_restake_earnings_updated`<br>`baker_stake_updated`<br>`delegation_configured` |
| Smart Contracts | `contract_initialized`<br>`contract_update_issued`<br>`module_deployed` |
| Data | `data_registered` |
| Transfer | `account_transfer`<br>`transferred_to_encrypted`<br>`transferred_to_public`<br>`encrypted_amount_transferred`<br>`transferred_with_schedule` |

!!! Note
    As a result, there is no direct link between transaction counts as shown in the graph and elsewhere, as failed transactions are not taken into account in the chart. 

2. We show these results in the [Today in](https://ccdexplorer.io/mainnet/today-in) page under the heading **Transaction Types**.

## Location
`nightrunner - TransactionTypes`