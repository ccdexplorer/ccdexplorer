# Transaction fees

Fees paid on the chain, in CCD, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_transaction_fees` |
| Field | `fee_for_day` |

Summed upstream from the cost of every account transaction in that day's
blocks. Updates and account creations generate no fee and are excluded. The
pipeline is described in
[Statistics Transaction Fees](../projects/timed_services/statistics_fees.md).

## Calculation

The field is stored in **microCCD**. The chart divides by 1 000 000 to show
CCD.

## Aggregation

Fees are collected during the period, so grouping by week or month **sums**
the daily totals.

A period covering fewer days than the rest holds a smaller total; such periods
are drawn faded and identified on hover.

## Distinctions

Fees actually paid, which depends on what was transacted. For the cost of one
specific operation over time, see
[Fee stabilization](fee_stabilization.md).

## Provenance

| | |
|---|---|
| Producer | Dagster `tx_fees` asset, nightrunner |
| Frequency | one document per day |
