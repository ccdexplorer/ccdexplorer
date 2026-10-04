# Network activity

CCD transferred per period, with transactions per second.

## Source

Two collections, joined on the date.

| Series | Collection | Field |
|---|---|---|
| Activity | `statistics`, type `statistics_network_activity` | `network_activity` |
| TPS | `statistics`, type `statistics_mongo_transactions` | `account_transaction` |

A day present in the first but not the second carries no TPS value; the
activity bar is still drawn.

## Calculation

Activity is the stored value, in CCD.

```
TPS = account_transaction / 86 400
```

The transaction count for the period divided by the seconds in a day. For
weekly and monthly groupings the divisor remains 86 400, so the figure is the
mean transactions per second across the days in the period rather than per
second of the whole period.

## Aggregation

- **Activity** is CCD moved during the period and is **summed**.
- **TPS** is derived from a summed count divided by a per-day constant, giving
  the period's daily mean rate.

## Presentation

Activity is drawn as bars and TPS as a line on a second vertical axis: a rate
below one and an amount in the millions share no useful scale.

## Distinctions

Activity measures CCD moved, which counts the same CCD again each time it
moves and includes transfers between accounts under one owner. It is a measure
of movement, not of economic value exchanged.

## Provenance

| | |
|---|---|
| Producers | Dagster `network_activity` and `mongo_transactions` assets |
| Frequency | one document per day from each |
