## Goal
The goal of this service is to extract relevant information from transactions for every block. Information includes logged events and impacted addresses from these and from the transactions itself. 

## Data used
HalloAs source we use blocks from the `blocks` collection and transactions from the `transactions` collection, as filled by the [Heartbeat](heartbeat.md) service.

For every transaction, we find which addresses (account, contract or public key) are **impacted** by this transaction and add this to the collection. 

## Calculation steps
Calculation happens separately for each grouping (daily, weekly, monthly). All three groupings use the same pipeline, only the `height_for_first_block` and `height_for_last_block` are selected differently (but all taken from the collection `blocks_per_day`)

### Daily
For `daily`, it's an entry every day. The timestamp on the entry is the day itself. 

### Weekly
For `weekly`, we take the starting block on Monday and end with the last block the following Sunday. The timestamp on the entry is the Monday of the week. During the current week, the week is marked as `incomplete`, meaning that for every day of this week, the data will be recalculated. It is marked `complete` when the entire week is in the past. 

### Monthly
For `monthly`, we take the starting block on the first day of the month and end with the last block of the last day of the month. The timestamp on the entry is the first day of the month. During the current month, the month is marked as `incomplete`, meaning that for every day of this month, the data will be recalculated. It is marked `complete` when the entire month is in the past. 

The following pipeline is used to extract the data:
```
pipeline = [
            {
                "$match": {
                    "block_height": {
                        "$gte": height_for_first_block,
                        "$lte": height_for_last_block,
                    },
                    "effect_type": {"$ne": "Account Reward"},
                }
            },
            {
                "$project": {
                    "address_length": {"$strLenCP": "$impacted_address_canonical"},
                    "impacted_address_canonical": 1,
                }
            },
            {
                "$group": {
                    "_id": {
                        "category": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$address_length", 29]},
                                        "then": "contract",
                                    },
                                    {
                                        "case": {
                                            "$and": [
                                                {"$gte": ["$address_length", 29]},
                                                {"$lt": ["$address_length", 64]},
                                            ]
                                        },
                                        "then": "address",
                                    },
                                    {
                                        "case": {"$eq": ["$address_length", 64]},
                                        "then": "public_key",
                                    },
                                ],
                                "default": "other",
                            }
                        }
                    },
                    "unique_addresses": {"$addToSet": "$impacted_address_canonical"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id.category",
                    "count": {"$size": "$unique_addresses"},
                }
            },
        ]
```
!!! Explanation: 
    `impacted_address_canonical` is the field that is filled with the canonical version of the address (only applicable for account addresses). The length of this field determines the category. 
!!! Note
    Payday Rewards are not taken into account as this is an automated system and does not indicative of actual intent by users. 

The output is a a dictionary (`counts_by_category`) with keys: `address`, `contract`, `public_key` and `counts` as value.

This is stored in the `statistics` collection as a dictionary:
```
{
"_id": _id,
"date": d_date,
"type": analysis.value,
"unique_impacted_address_count": counts_by_category,
}
```

Where analysis is:

|Grouping|Analysis|
|-|-|
|Daily|statistics_unique_addresses_v2_daily|
|Weekly|statistics_unique_addresses_v2_weekly|
|Monthly|statistics_unique_addresses_v2_monthly|
