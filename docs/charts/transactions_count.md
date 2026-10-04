# Transactions

Account transactions, grouped into five categories, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_mongo_transactions` |
| Fields | one count per transaction type |

Each document holds a count per transaction type for that day. A type absent
from a document did not occur; it is read as zero.

## Calculation

The five categories are sums of the underlying types:

| Category | Types counted |
|---|---|
| Account | `account_creation`, `credential_keys_updated`, `credentials_updated` |
| Transfer | `account_transfer`, `transferred_to_encrypted`, `transferred_to_public`, `encrypted_amount_transferred`, `transferred_with_schedule` |
| Smart Contracts | `contract_initialized`, `contract_update_issued`, `module_deployed` |
| Staking | `baker_configured`, `baker_added`, `baker_removed`, `baker_keys_updated`, `baker_restake_earnings_updated`, `baker_stake_updated`, `delegation_configured` |
| Data | `data_registered` |

Types outside these five — chain updates, for instance — are not drawn. The
stack is therefore account transactions by category, not all transactions.

What each underlying type records is described in
[Transactions by Type/Contents](../projects/timed_services/transactions_by_type_contents.md).

## Aggregation

Counts of transactions occurring in the period, so grouping by week or month
**sums** the daily counts.

A period covering fewer days than the rest holds a smaller total; such periods
are drawn faded and identified on hover.

## Presentation

Stacked bars: the categories partition the same set of transactions, and the
stack height is their total. Categories can be turned off individually, which
changes the total drawn.

## Provenance

| | |
|---|---|
| Producer | Dagster `mongo_transactions` asset, nightrunner |
| Frequency | one document per day |
