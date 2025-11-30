# Accounts Retrieval (Project: `accounts_retrieval`)

The Accounts Retrieval project produces immutable daily snapshots of every Concordium account and CIS-2 token balance.  
It runs as a batch process that talks directly to the node (via `GRPCClient`) and MongoDB to reconstruct the full ledger
for a selected block height.

## Responsibilities
- Walk finalized blocks to collect every account address that has ever existed (genesis accounts are seeded manually).
- For a given block hash/height, call `GetAccountInfo` for each address and capture balances, stake status, credential data,
  and CIS-2 token holdings via `ccdexplorer.grpc_client.CCD_Types.CCD_AccountInfo`.
- Persist summarized CSV exports (`data/balances/YYYY-MM-DD.csv`) that downstream analytics jobs (e.g. realized prices) read via Git history.
- Store derived per-day artifacts in MongoDB so other jobs (Dagster runs, API backfills) can reuse the canonical numbers.
- Publish status updates through `Tooter` so operators know when a snapshot finished or failed.

## Workflow
1. Determine the block height/hash for the requested date (usually the last finalized block of that day).
2. Query MongoDB’s `transactions` collection for every `account_creation` up to that height and merge with the predefined list of genesis accounts.
3. For each address, call `grpcclient.get_account_info` to build an `Account` object that records balances, staking role,
   credentials, and CIS-2 token balances (including decimals). Canonical account IDs (first 29 characters) are used so that aliases
   collapse to a single logical record.
4. Produce per-day Pandas/Polars data frames via `get_accounts_for_day_from_blocks_collection` and `get_plts`, then serialize to CSV.
5. Commit the CSV to Git and push (the job shells out via `gitpython`’s `Repo` helper) so historical states are traceable.
6. Store metadata documents in MongoDB and post a success/failure notification through `Tooter`.

## Outputs
- `data/balances/<date>.csv` – full ledger snapshot with balances, aliases, and token holdings.
- MongoDB helper documents that indicate the last processed block height per date.
- `Tooter` notifications describing the run status and, if needed, error context.

## Deployment
The Dockerfile under `projects/accounts_retrieval/` packages the batch app. Typical runs are executed
manually or via a scheduler (e.g. Dagster) in coordination with nightly analytics jobs so the exported CSVs are immediately available
to downstream tasks such as [`dagster_nightrunner`](dagster_nightrunner.md).***
