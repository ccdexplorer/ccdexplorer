
## Goal
The goal of this script is to calculate, on a daily basis, which recognized projects have contributed to the chain. For each [Account Transaction](../../entities/transaction.md/#account-transaction), we have a possible `recognized_sender_id`, that gets filled when the `account_transaction.sender` can be found in `projects.json`. 

## Data used
The source for this calculation are the transactions as stored in the collection `transactions`. Note that in the [Heartbeat](../every_block/heartbeat.md) service, we try to tag every transactions that is sent from a recognized project with the property `recognized_sender_id`. That field then contains the `project_id`. 

!!! Note
    When an update is performed on `projects.json` and a new `account_address` is added, this service will go through all days of the chain, find transactions that have not yet been recognized (property `recognized_sender_id` does not exist for the transaction), and will try to match these transactions against the update `projects.json` file. When it finds a day where a transaction can now be recognized, it marks this day as `incomplete`.

## Calculation steps
The first step is to determine which days we need to run this pipeline for. We start with all days from the chain. Then we check for which of those days we haven't performed this calculation. Finally we add days that are marked as `incomplete`. 

For every day the following pipeline is run:
```
pipeline = [
            {
                "$match": {
                    "recognized_sender_id": {"$exists": True},
                    "block_info.height": {
                        "$gte": height_for_first_block,
                        "$lte": height_for_last_block,
                    },
                }
            },
            {
                "$group": {
                    "_id": {
                        "project_id": "$recognized_sender_id",
                        "type_contents": "$type.contents",
                    },
                    "count": {"$sum": 1},
                }
            },
            {
                "$group": {
                    "_id": "$_id.project_id",
                    "type_counts": {
                        "$push": {"type": "$_id.type_contents", "count": "$count"}
                    },
                }
            },
            {"$project": {"_id": 0, "project_id": "$_id", "type_counts": 1}},
        ]
```


The `contents` field is filled with all properties of [CCD_AccountTransactionEffects](../../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_AccountTransactionEffects), combined with [CCD_RejectReason](../../components/grpc.md/#ccdexplorer.grpc_client.CCD_Types.CCD_RejectReason).


## Examples
This is an example for project 'Aesirx" for 2025-02-18:
```
{
  "_id": "2025-02-18-statistics_transaction_types_by_project-aesirx",
  "date": "2025-02-18",
  "type": "statistics_transaction_types_by_project",
  "project": "aesirx",
  "tx_type_counts": {
    "data_registered": 512,
    "contract_update_issued": 2
  }
}
```
And the corresponding 'helper' document to keep track of whether the day is `complete` or `incomplete` looks like:
```
{
  "_id": "statistics_transaction_types_by_project_dates-2025-02-18",
  "date": "2025-02-18",
  "type": "statistics_transaction_types_by_project_dates",
  "complete": true
}
```

## Where is this data used?
TODO

## Location
`nightrunner - TxsByProject`