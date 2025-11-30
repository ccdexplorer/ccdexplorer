# CCDExplorer API (Project: `ccdexplorer_api`)

The public API is built on [FastAPI](https://fastapi.tiangolo.com/), with all data retrieved directly from the node, or from MongoDB helper collections populated by the background services listed in this documentation.

Check the [documentation](https://api.ccdexplorer.io/docs) for the Swagger docs for all endpoints. 

For information on the schemas, check the [grpc](../components/grpc.md) page.

## Responsibilities
- Serve explorer UI requests (accounts, blocks, statistics, project metadata, token holdings).
- Provide authenticated endpoints (API key header) for partners to query advanced statistics and notification settings.
- Orchestrate alias-aware lookups by leveraging canonical IDs from `all_account_addresses`.

## Dependencies
- [`components/grpc_client`](../components/grpc.md) for live Concordium data.
- MongoDB collections populated by [`heartbeat`](every_block/heartbeat.md), [`ms_indexers`](every_block/indexers.md), [`ms_token_accounting`](every_block/token_accounting.md), etc.
- [`site_user`](../components/site-user.md) models for user preference endpoints.

See the repository under `projects/ccdexplorer_api/` for routers, templates, and Dockerfile details.***
