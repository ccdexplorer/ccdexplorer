# Active addresses

Distinct addresses that sent or received in a period.

## Source

| | |
|---|---|
| Collection | `statistics` |
| Types | `statistics_unique_addresses_v2_daily`, `..._weekly`, `..._monthly` |
| Fields | `unique_impacted_address_count.address`, `.contract`, `.public_key` |

Three series: native accounts, smart contracts, and CIS-5 public keys. The
`public_key` field appears only in documents from after CIS-5 wallets existed
and is absent earlier.

## Aggregation

This chart does not aggregate. A distinct count cannot be derived from
narrower ones — an address active on Monday and Tuesday is one address for the
week, not two — so the daily, weekly and monthly totals are each computed
upstream over their own period, and changing the grouping **selects a
different collection** rather than combining documents.

The pipeline that builds them is described in
[Unique Active Addresses](../projects/timed_services/statistics_unique_addresses.md).

## Distinctions

Counts addresses that appear in a transaction as sender or as an impacted
party. It is not a count of users: one person may hold many accounts, and one
account may be an exchange serving many.

## Provenance

| | |
|---|---|
| Producer | Dagster, three assets writing one document per period |
| Frequency | one document per day, per week and per month |
