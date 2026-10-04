# Average delegator stake

Mean stake held by a delegating account, in CCD, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_classified_pools` |
| Field | `delegator_avg_stake` |

Computed upstream as total delegated stake divided by the number of
delegators. Stored in CCD; no conversion is applied at render time.

## Aggregation

The stored value is already an average. Grouping by week or month takes the
**mean of the daily values** over the period.

As with any mean of means, the result is not weighted by the number of
delegators on each day and differs slightly from recomputing the average over
the whole period.

## Provenance

| | |
|---|---|
| Producer | Dagster `classified_pools` asset |
| Frequency | one document per day |
