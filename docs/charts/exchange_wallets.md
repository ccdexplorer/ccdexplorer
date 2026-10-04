# Exchange wallets

Count of known wallets per exchange, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_exchange_wallets` |
| Fields | one per exchange, as for [CCD on exchanges](ccd_on_exchanges.md) |

The number of addresses attributed to each exchange — deposit addresses and
aliases included — at that day's final block.

## Aggregation

A count in existence, not a count of events. Grouping by week or month uses
**the last day in the period**.

## Presentation

One line per exchange rather than a stack: the counts are independent of each
other and differ by two orders of magnitude, so a total would be dominated by
whichever exchange issues the most deposit addresses.

## Distinctions

Counts addresses, not customers or balances. An exchange issuing a fresh
deposit address per customer will show a rising line regardless of how much
CCD it holds; for the amounts, see [CCD on exchanges](ccd_on_exchanges.md).

## Provenance

| | |
|---|---|
| Producer | Dagster `exchange_wallets` asset, nightrunner |
| Frequency | one document per day |
