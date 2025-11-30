# Dagster Recurring (Project: `dagster_recurring`)

`dagster_recurring` contains the frequently executed maintenance jobs that keep Explorer secondary indexes fresh between nightly runs.

## Key jobs
- **Dashboard nodes** – refreshes validator/node telemetry so the site and bot can surface lagging validators or version drift.
- **Market position** – recalculates holdings for curated wallet cohorts (e.g. smart contract treasuries) to power the “Market Position” widgets.
- **Memos** – syncs memo text from transactions into helper collections, capturing the new memo height so consumers can resume efficiently.
- **Top impacted addresses** – recomputes impacted-address leaderboards using the data produced by `ms_events_and_impacts`.
- **Validators missed** – checks for validators that missed rounds or are close to suspension and raises alerts.
- **Transaction type counts** – tallies transactions per type to backfill statistics exposed by the API.
- **Metadata & spot retrieval** – reruns CIS-2 metadata parsing or market spot price fetching whenever a previous attempt failed.
- **Redis failures** – audits Celery queues and Redis streams for stuck change streams or resume token drift.

## Execution model
- Jobs are typically scheduled multiple times per day (hourly or every few hours) depending on their SLA.
- Each asset communicates with MongoDB (and, when needed, Redis or external APIs) and updates helper documents such as
  `heartbeat_last_block_processed_transactions_types_count` to signal progress.
- Alerts are routed through `Tooter`, ensuring operational visibility across the fleet.

## Deployment
- Packaged via `projects/dagster_recurring/Dockerfile`, which runs `dagster api grpc -m ccdexplorer.dagster_recurring.repository -a defs`.
- Register the image as a Dagster code location alongside `dagster_nightrunner` to share infrastructure while keeping schedules isolated.***
