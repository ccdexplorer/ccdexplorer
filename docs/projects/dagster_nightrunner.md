# Dagster Nightrunner (Project: `dagster_nightrunner`)

`dagster_nightrunner` is the nightly analytics code location. It aggregates Concordium data into time-series
datasets that power the “Timed Services” dashboards and scheduled refresh jobs.

## Scope
- **Ledger analytics** – transactions by type/contents, transaction fee buckets, realized price calculations, and microCCD supply tracking.
- **Account & network views** – account graph topology, exchange wallet classifications, unique active accounts, classified pools, network activity summaries.
- **Project intelligence** – transactions grouped by recognized projects, refreshed `projects.json` snapshots, and MongoDB extracts used by the Explorer site.
- **External data fusion** – ingestion of historical FX rates and release schedules so on-chain values can be expressed in fiat terms.

Every workload is packaged as a Dagster asset/job under `ccdexplorer.dagster_nightrunner.src.*` and exposed via
[`Definitions.merge`](https://docs.dagster.io/concepts/definitions) in `repository.py`.

## Execution model
- Runs are orchestrated nightly (typically shortly after the `accounts_retrieval` snapshot) so they operate on a consistent view of the chain.
- Each asset reads from MongoDB or the CSV exports under `data/balances/`, performs its aggregation (Pandas/Polars/Dagster IO managers), and writes the result back to MongoDB or to helper collections such as `statistics_*`.
- Notifications and monitoring leverage the shared `Tooter` infrastructure so failures are surfaced quickly.

## Deployment
- The Dockerfile under `projects/dagster_nightrunner/` installs the project plus all component dependencies (GRPC client, CIS parser, Mongo access).
- In production, this image is registered as a Dagster code location and scheduled/coordinated via Dagster’s `jobs` defined in `ccdexplorer.dagster_nightrunner.src._jobs`.
- Local development can run `uv pip install -e ./projects/dagster_nightrunner` and then `dagster dev -m ccdexplorer.dagster_nightrunner.repository` to load the definitions.***
