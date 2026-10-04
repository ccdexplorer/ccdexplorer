# Delegator count

Accounts delegating stake to a validator pool, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_classified_pools` |
| Field | `delegator_count` |

Counts delegations to validator pools. Passive delegation is held separately
and is not included.

## Aggregation

A snapshot. Grouping by week or month uses **the last day in the period**. The
counts are not summed: an account delegating all week is one delegator, not
seven.

## Provenance

| | |
|---|---|
| Producer | Dagster `classified_pools` asset |
| Frequency | one document per day |
