## Goals
The goal of this service is to calculate fees from `account_transactions` for each day.

## Data used
We use all transactions from the `transactions` collection and the aggregate information on blocks from the `block_per_day` collection.

## Calculations
The following pipeline is executed on the `transactions` collection. Note that it only looks for `account_transactions`, as `update` and `account_creation` do not generate fees.


```
            pipeline = [
                {"$match": {"account_transaction": {"$exists": True}}},
                {
                    "$match": {
                        "block_info.height": {
                            "$gte": height_for_first_block,
                            "$lte": height_for_last_block,
                        }
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "fee_for_day": {"$sum": "$account_transaction.cost"},
                    }
                },
            ]
```

## Examples
TODO

## Location
`Nightrunner - TransactionFees

