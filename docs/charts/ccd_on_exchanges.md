# CCD on exchanges

CCD held in known exchange wallets, by exchange, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_ccd_classified` |
| Fields | one per exchange: `bitfinex`, `bitglobal`, `mexc`, `ascendex`, `kucoin`, `coinex`, `lcx`, `gate.io`, `bitmart`, `kraken` |

Balances in CCD, summed over the wallets attributed to each exchange at that
day's final block. Attribution is maintained by ccdexplorer; an exchange
wallet that has not been identified is not counted.

## Aggregation

A balance is a level. Grouping by week or month uses **the last day in the
period**; summing successive days of a balance would invent CCD.

## Presentation

A stacked area: the band heights are the point, and the outline is total CCD
held across all tracked exchanges. Exchanges can be turned off individually,
which changes the total drawn.

## Distinctions

Covers identified wallets of the listed exchanges only. Movement in the total
reflects deposits and withdrawals, and also any change in which wallets are
attributed.

## Provenance

| | |
|---|---|
| Producer | Dagster `ccd_classified` asset, nightrunner |
| Frequency | one document per day |
