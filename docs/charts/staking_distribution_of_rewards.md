# Reward distribution

Staking rewards paid out, split by recipient type, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_daily_payday` |
| Fields | `total_rewards_validators`, `total_rewards_pool_delegators`, `total_rewards_passive_delegators` |

Amounts are in CCD and cover the paydays occurring on that day. Rewards are
distributed per payday rather than per block, so a day's figure is the sum of
the paydays it contained.

## Aggregation

Rewards are amounts paid during the period, so grouping by week or month
**sums** the daily values.

A period covering fewer days than the rest — the first or last in a range —
therefore holds a smaller total. Such periods are drawn faded and identified
on hover.

## Presentation

Stacked bars: the three series are parts of one payout, and the stack height
is total rewards distributed.

## Provenance

| | |
|---|---|
| Producer | Dagster paydays |
| Frequency | one document per day |
