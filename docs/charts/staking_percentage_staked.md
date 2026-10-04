# Percentage staked

Share of the total CCD supply that is staked, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_ccd_classified` |
| Fields | `staked`, `total_supply` |

Both figures are in CCD and describe the state at each day's final block.
`staked` covers validator stake and delegated stake together.

## Calculation

```
percentage = staked / total_supply × 100
```

The ratio is taken after aggregation, not before: for a weekly point it is the
week's closing staked amount over the week's closing supply, which is the
share actually in force at that moment. Averaging the daily ratios would give
a slightly different number with no corresponding instant.

## Aggregation

Both inputs are snapshots. Grouping by week or month uses **the last day in
the period**.

## Provenance

| | |
|---|---|
| Producer | Dagster `ccd_classified` asset |
| Frequency | one document per day |
