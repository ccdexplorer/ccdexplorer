# Restaked rewards

Share of staking rewards that was restaked rather than paid out, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_daily_payday` |
| Field | `restaked_rewards_perc` |

Computed upstream as restaked rewards over total rewards for the day. Stored
as a fraction, not a percentage: a value of 0.79 means 79 per cent.

Whether rewards are restaked is a per-account setting, so the figure moves
with the composition of stake rather than with any protocol parameter.

## Aggregation

The stored value is already a ratio. Grouping by week or month takes the
**mean of the daily values**.

It is not summed: seven daily fractions added together would exceed 1 and
describe nothing. The mean is unweighted by the size of each day's payout.

## Provenance

| | |
|---|---|
| Producer | Dagster paydays |
| Frequency | one document per day |
