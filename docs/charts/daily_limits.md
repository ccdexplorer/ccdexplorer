# Daily limits

CCD required to hold a place in the top 100 and top 250 accounts by balance,
per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_daily_limits` |
| Fields | `amount_to_make_top_100`, `amount_to_make_top_250` |

The balance of the hundredth and two hundred and fiftieth largest account,
ranked at each day's final block. Amounts are in CCD.

## Aggregation

A threshold in force at an instant. Grouping by week or month uses **the last
day in the period**; the values are not summed or averaged across days.

## Distinctions

A ranking threshold, not a holding. No account necessarily holds exactly this
amount, and the line moves when accounts near the boundary trade places as
well as when balances change.

Accounts are ranked individually. An entity holding several accounts appears
several times.

## Provenance

| | |
|---|---|
| Producer | Dagster `daily_limits` asset, nightrunner |
| Frequency | one document per day |
