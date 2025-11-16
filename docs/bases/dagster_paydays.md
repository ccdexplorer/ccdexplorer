# Dagster Payday base

The `ccdexplorer.dagster_paydays` base wires together multiple Dagster definitions so the
payday accounting flow can run as a single code location. Its `repository.py` merges the
assets and jobs exported from `src.paydays`, `src.paydays_day_info`, `src.paydays_averages`,
and `src._jobs`, which gives the Dagster UI a single `Definitions` object to load.

## Core asset: `paydays_calculation_daily`
The heart of the base lives in `src/paydays.py`. The `@asset` named `paydays_calculation_daily`
uses the `from_staking` partitions to process one staking epoch at a time, pulls metadata from
the latest `paydays_day_info` materialization, and orchestrates all payday routines:

1. `perform_payday_init()` seeds MongoDB with the selected partition's payday snapshot.
2. `perform_payday_state_information_for_current_payday()` enriches the snapshot with gRPC
   state, such as the validator set.
3. The sequence of `perform_payday_performance_for_bakers()`, `perform_payday_apy_calc_for()`,
   and `perform_payday_rewards()` calculates distribution metrics for validators, delegators,
   and passive stake.
4. `fill_daily_apy_for_accounts_for_date()` and `fill_daily_apy_for_validators_for_date()`
   persist the APY curves for each cohort before `save_statistics_for_date()` stores the
   derived statistics.

The materialization returns metadata such as total rewards (pooled, validator, delegator, and
passive breakdowns), per-cohort daily APYs, staked balances, and validator/delegator counts so
the Dagster run logs show the exact payday health indicators that downstream dashboards use.

## Resource management
`src/_resources.py` provides two reusable `ConfigurableResource` classes:

- `MongoDBResource` injects the shared `MongoDB` brick via a `Tooter` notifier and can be
  configured per deployment if needed.
- `GRPCResource` exposes the Concordium `GRPCClient` so the assets do not have to recreate a
  client for each op.

The asset accesses them via the `ResourceParam` dependencies, which ensures a single client is
reused across the payday orchestration steps.

## Event-driven jobs
At the bottom of `paydays.py` an `@asset_sensor` watches the `paydays_calculation_daily` asset
and triggers the `job_payday_averages` job whenever a partition finishes. This keeps the derived
average calculations in sync without manual scheduling while still allowing Dagster to manage the
resulting runs.
