# Holders

How many accounts hold at least 1M, 5M, 10M, 50M and 100M CCD, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_daily_holders` |
| Fields | `count_>=1000000`, `count_>=5000000`, `count_>=10000000`, `count_>=50000000`, `count_>=100000000` |

Each document holds a count and a total for thirteen balance thresholds — 0,
100, 10 000, 20 000, 50 000, 100 000, 500 000, 1M, 2.5M, 5M, 10M, 50M and
100M. This chart draws five of them.

The other eight are in the data and not on the chart. 0 and 100 are
effectively "every account that exists", and the thresholds between 10 000
and 500 000 are large enough counts that they flatten the five drawn here
against the bottom of the axis. Each is available in the collection for
anyone querying it directly.

## Aggregation

Snapshots. Grouping by week or month uses **the last day in the period**,
never a sum — each line is a standing count, so adding seven days together
would multiply it by seven.

## Reading it

Each threshold contains the one above it: every account over 100M CCD is also
counted in the 1M line. The lines are nested, not exclusive, so the gap
between two of them is the number of accounts between those balances.

## Distinctions

Counts accounts, not holders. One entity controlling several accounts is
counted once per account; an exchange account holding many customers' CCD is
counted once.

The threshold is on balance alone and includes staked and delegated amounts.

## Provenance

| | |
|---|---|
| Producer | Dagster `daily_holders` asset, nightrunner |
| Frequency | one document per day |
