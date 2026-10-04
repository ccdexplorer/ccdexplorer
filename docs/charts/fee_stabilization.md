# Fee stabilization

Cost of a plain CCD transfer — one account to another, no memo, no contract
call — expressed in CCD, per day.

Concordium prices transaction fees in euros. A transfer consumes a fixed
amount of energy, energy has a price in euros, and the chain converts euros to
CCD using an exchange rate carried in the chain parameters. The fee in euros is
therefore near-constant while the fee in CCD varies inversely with the CCD
price.

## Source

Two chain parameters, read from the chain itself. No transaction data and no
external price feed are involved; the chart is defined on days with no
transfers.

A nightly job reads the chain parameters as at **the last block of each day**
and writes one document per day to the `statistics` collection under the type
`statistics_microccd`:

| Field | Chain parameter | Quantity |
|---|---|---|
| `GTU_numerator` / `GTU_denominator` | `micro_ccd_per_euro` | microCCD per euro |
| `NRG_numerator` / `NRG_denominator` | `euro_per_energy` | euros per unit of energy |

Both parameters are exact fractions. The numerators exceed 64 bits — the value
stored for 1 October 2026 is `12259715872310272000` — so all four fields are
held as strings and converted to floating point at render time.

## Calculation

A plain transfer consumes **501 energy**. The remainder is unit conversion:

```
cost_ccd = 501
         × NRG_numerator / NRG_denominator      euros per energy
         × GTU_numerator / GTU_denominator      microCCD per euro
         ÷ 1 000 000                            microCCD to CCD
```

Applied to the values stored for 1 October 2026:

| Quantity | Derivation | Value |
|---|---|---|
| Energy per transfer | constant | 501 |
| Euros per energy | `1 / 50 000` | 0.00002 |
| Cost in euros | `501 × 0.00002` | 0.01002 |
| MicroCCD per euro | `12259715872310272000 / 39688763837` | 308 896 390 |
| CCD per euro | `308 896 390 / 1 000 000` | 308.90 |
| Cost in CCD | `0.01002 × 308.90` | **3.095142** |

## Aggregation

Grouping by week or month takes **the last day in the period** — the four
parameters as at that day's final block — and applies the calculation to those
values.

The parameters are a rate in force at an instant, not a quantity that
accumulates, so they are not summed: a weekly sum would report seven times the
prevailing rate. A mean over the period is also well defined; the closing value
is used because it answers what a transfer cost at the end of the period.

All charts end at the previous day. Each day's figure derives from that day's
final block, which does not exist until the day is complete.

## Presentation

- The vertical axis is logarithmic. The cost has spanned several orders of
  magnitude; on a linear axis most of the history collapses to the baseline.
- The series is stepped rather than continuous. The exchange rate changes by
  update transaction, so the cost is constant between updates.

## Distinctions

- **Not a market price.** The CCD/EUR rate here is the chain's own parameter,
  set by update transaction. It tracks the market closely but is not derived
  from trading. Traded prices are shown by the CCD on Kraken chart.
- **Not observed fees.** This is the cost of one specific operation, not an
  average of fees actually paid, which depends on the mix of transaction types.
  Fees collected are shown by
  [Transaction fees](../projects/timed_services/statistics_fees.md).

## Provenance

| | |
|---|---|
| Collection | `statistics`, type `statistics_microccd` |
| Producer | Dagster `microccd` asset, nightrunner, one document per day |
| Chain call | `GetBlockChainParameters` at each day's final block |
| Energy constant | 501, the cost of a plain transfer |
