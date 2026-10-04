# Open pools

Validator pools accepting delegation, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_classified_pools` |
| Field | `open_pool_count` |

A pool is counted as open when its validator has set its open status to accept
new delegators. Pools that are closed for new delegation, or closed
altogether, are counted separately in the same document and are not shown
here.

## Aggregation

A snapshot. Grouping by week or month uses **the last day in the period**.

## Provenance

| | |
|---|---|
| Producer | Dagster `classified_pools` asset |
| Frequency | one document per day |
