# Accounts growth

Accounts on the chain, and the number created each day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_network_summary` |
| Field | `account_count` |

A running total of accounts in existence at each day's final block. The chart
draws two series from this one field.

## Calculation

```
Accounts On Chain = account_count                                  running total
Account Growth    = account_count − account_count of the period before
```

Growth is derived, not stored. The chain records how many accounts exist, not
how many were created, so the daily figure is the difference between
consecutive totals.

The first period in any range has nothing to subtract from and carries no
growth value.

## Aggregation

The two series aggregate differently from the same field:

- **Accounts On Chain** is a snapshot: the period's **last day**.
- **Account Growth** is the difference between the period's closing total and
  the previous period's, so a weekly point is the accounts created that week.

Growth is understated by a period covering fewer days than the rest. Such
periods are drawn faded and identified on hover.

## Distinctions

Counts native accounts only. Smart-contract wallets holding CIS-5 balances are
not accounts on the chain and are not included.

## Provenance

| | |
|---|---|
| Producer | Dagster `network_summary` asset, nightrunner |
| Frequency | one document per day |
