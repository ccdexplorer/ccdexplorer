# Env Settings (`components/ccdexplorer/env`)

The `env` component centralizes every environment variable consumed by CCDExplorer services.
Importing `ccdexplorer.env` exposes typed constants (e.g. `MONGO_URI`, `RUN_ON_NET`, `GRPC_MAINNET`) so each project
uses the same defaults regardless of runtime (local, staging, production).

## Highlights
- Wraps Python-dotenv / OS environment reads inside `settings.py`, keeping secrets and configuration keys in one place.
- Provides Concordium node endpoints (`GRPC_MAINNET`, `GRPC_TESTNET`) so both gRPC clients and concordium-client helpers can share the same pool.
- Supplies messaging credentials (`NOTIFIER_API_TOKEN`, `API_TOKEN`, Fastmail fields) for the notification stack.
- Defines operational constants such as `HEARTBEAT_PROGRESS_DOCUMENT_ID`, `MAX_BLOCKS_PER_RUN`, `BLOCK_COUNT_SPECIALS_CHECK`, and `TX_REQUEST_LIMIT_DISPLAY`.
- Indicates runtime discovery helpers (`environment()`, `ON_SERVER`, `RUN_LOCAL_STR`) that other services use to tweak logging or scheduling.

## Usage
```python
from ccdexplorer import env

redis_url = env.REDIS_URL
run_on_net = env.RUN_ON_NET  # "mainnet" or "testnet"
heartbeat_doc_id = env.HEARTBEAT_PROGRESS_DOCUMENT_ID
```

Because every project imports the same module, rotating credentials or node lists can be done in a single place without
touching dozens of services.***
