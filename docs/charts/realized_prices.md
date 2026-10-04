# Realized price

Average price at which the circulating supply last moved, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_realized_prices` |
| Field | `realised_price` |

Realized price is realized capitalisation divided by circulating supply, where
realized capitalisation values every unit of CCD at the price when it last
moved on-chain rather than at the current price. The method, and how the
per-unit cost basis is tracked, is described in
[Realized Prices](../projects/timed_services/statistics_realized_prices.md).

## Aggregation

A valuation at an instant. Grouping by week or month uses **the last day in
the period**.

## Distinctions

Not a market price. It moves when coins change hands, and a coin that has not
moved for years continues to be valued at the price it last moved at. It is
commonly read as an aggregate cost basis of the supply.

## Provenance

| | |
|---|---|
| Producer | Dagster `realized_prices` asset, nightrunner |
| Frequency | one document per day |
