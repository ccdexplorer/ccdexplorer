# Validator count

Validators registered on the chain, split into those currently active and
those suspended, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_network_summary` |
| Fields | `validator_count`, `suspended_count` |
| Chain call | `GetTokenomicsInfo` at each day's final block |

`suspended_count` was introduced in March 2025. Documents written before then
do not carry the field; it is read as zero, so the active count equals the
registered count for that period.

## Calculation

```
active = validator_count - suspended_count
```

Suspension was added with protocol version 8. A suspended validator remains
registered and keeps its stake, but is not selected to bake.

## Aggregation

Both counts are snapshots, taken at a single block. Grouping by week or month
uses **the last day in the period**; the counts are not summed, which would
report a multiple of the validators that exist.

## Presentation

Active validators are drawn as a line and suspended as bars: the second is a
subset of the first, not a comparable series.

## Provenance

| | |
|---|---|
| Producer | Dagster `network_summary` asset, nightrunner |
| Frequency | one document per day |
