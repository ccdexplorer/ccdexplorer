# CCD price charts

Two families of intraday price chart, neither of which is built from the
statistics collection and neither of which supports grouping or a date range.

## CCD on Kraken

Candles and volume from the Kraken order book, at seven intervals: 1m, 5m,
15m, 30m, 1h, 4h and 1d. Each draws the most recent 120 candles of its
interval, so the window covered follows from the interval — two hours at 1m,
120 days at 1d.

| | |
|---|---|
| Source | Kraken OHLC, via the ccdexplorer API |
| Pair | CCD/USD |

At the shorter intervals CCD is thinly traded and many candles contain no
trades.

Four hours is the interval the charts gallery and the chart bot open at; the
others are reached from it.

## CCD price

The chain's own CCD/EUR rate over the last 24 hours, 90 days or year,
converted to USD and closed on the current spot price.

| | |
|---|---|
| Source | `micro_ccd_per_euro`, the chain parameter |
| Updated | by update transaction, roughly every half hour |

This is the rate the chain uses to price transaction fees, not a market feed.
It tracks the market closely. It is the only complete intraday history of the
CCD price held by ccdexplorer: the daily job records one point per day, taken
shortly after midnight.

The same parameter, applied to the cost of a transfer, produces
[Fee stabilization](fee_stabilization.md).

## Aggregation

Neither family aggregates. The horizontal unit is the candle interval or the
raw sample, not a calendar period, so the grouping and range controls that
appear on other charts are absent.

## Provenance

| | |
|---|---|
| Kraken | API endpoint `/v2/mainnet/misc/ccd-ohlc/{interval}` |
| Chain rate | API endpoint `/v2/mainnet/misc/ccd-price/last/{hours}` |
