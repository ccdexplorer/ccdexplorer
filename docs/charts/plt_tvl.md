# PLT stablecoin TVL

Total value locked in Protocol Level Token stablecoins, in USD.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_plt` |
| Field | `tokens.<SYMBOL>.USD.total_supply` |

Each document holds one entry per PLT, keyed by symbol, with supply in the
token's own currency and converted to USD at that day's rate. Only tokens
declaring a `stablecoin_tracks` currency are included in the total; PLTs that
track nothing are excluded, since the chart is of stablecoins.

## Calculation

Per token the period's closing supply is taken, carried forward if the token
did not report that period, and the results are then summed. Summing first and
taking the closing value afterwards would drop a token that stopped reporting
to zero and understate the total.

## Aggregation

Supply is a level. Grouping by week or month uses **the last day in the
period**; adding successive days of a supply figure would invent money.

## Distinctions

Reports supply on Concordium only — not the total supply of these stablecoins
on their native chains.

## Provenance

| | |
|---|---|
| Producer | Dagster PLT assets |
| Frequency | one document per day |
