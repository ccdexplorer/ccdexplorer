# Delegators per pool

Mean number of delegators in a validator pool, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_classified_pools` |
| Field | `delegator_avg_count_per_pool` |

Computed upstream as the delegator count divided by the number of pools
holding delegation. Pools with no delegators are excluded from the
denominator, so the figure describes pools that are actually used.

## Aggregation

The stored value is already an average. Grouping by week or month takes the
**mean of the daily values** over the period, rather than the closing day,
which would discard the rest of the period.

The mean of daily averages is not weighted by pool count and so differs
slightly from recomputing the average over the whole period. The difference is
immaterial while the pool count is stable.

## Provenance

| | |
|---|---|
| Producer | Dagster `classified_pools` asset |
| Frequency | one document per day |
