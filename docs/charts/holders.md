# Holders

Accounts holding at least one million CCD, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_daily_holders` |
| Field | `count_>=1000000` |

Each document holds a count and a total for a series of balance thresholds —
0, 100, 10 000, 20 000 and upwards. This chart draws the one-million
threshold.

## Aggregation

A snapshot. Grouping by week or month uses **the last day in the period**.

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
