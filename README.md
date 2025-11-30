[![Build & Push Projects](https://github.com/ccdexplorer/ccdexplorer/actions/workflows/build-projects.yml/badge.svg?branch=main)](https://github.com/ccdexplorer/ccdexplorer/actions/workflows/build-projects.yml)

# CCDExplorer.io
CCDExplorer is the open-source Concordium blockchain explorer that powers [ccdexplorer.io](https://ccdexplorer.io), the public [API](https://api.ccdexplorer.io), and the user-facing [Notification Bot](https://ccdexplorer.io/settings/user/overview). The codebase collects on-chain data straight from a Concordium node, enriches it through background services, and serves it via FastAPI applications for browsers, bots, and programmatic clients.

## Highlights
- **Comprehensive Concordium coverage** – blocks, transactions, accounts, smart contracts, and modules are indexed in real time, with every-block workers tracking chain health, new accounts, and new or upgraded contracts/modules.
- **Developer-friendly API** – FastAPI-powered endpoints expose raw chain data alongside derived metrics for integrations, trading dashboards, or research. Public Swagger docs are available at [/docs](https://api.ccdexplorer.io/docs), with schemas derived from the gRPC definitions that the components share.
- **Notifications and user tools** – the notification bot lets users subscribe to account, contract, or chain events directly from ccdexplorer.io, backed by the same services that drive the explorer frontend.
- **On-chain analytics** – scheduled jobs compute data sets such as transactions by type, per-project usage, unique address counts, daily holder & limit statistics, and realized price series to give Concordium participants contextual insights.
- **Polylith monorepo** – the repository uses [Polylith for Python](https://github.com/DavidVujic/python-polylith) to keep components, bases, and projects isolated yet composable, making it easy to reuse bricks across services.

## Repository layout
Key Polylith bricks live under `projects/` (deployable services) and `components/` (shared code). Below is the actual layout currently in use:

### Projects
| Path | Description |
| --- | --- |
| `projects/ccdexplorer_site` | FastAPI + Jinja application serving the public explorer UI via the `ccdexplorer.ccdexplorer_site` base. |
| `projects/ccdexplorer_api` | FastAPI service that exposes the documented REST API backed by direct Concordium node queries and MongoDB. |
| `projects/ccdexplorer_bot` | Telegram/email notification bot and user settings backend. |
| `projects/accounts_retrieval` | Batch job that snapshots accounts/tokens per day straight from the node and stores CSV + Mongo states for analytics. |
| `projects/heartbeat` | Real-time finalized-block listener that writes blocks/transactions into MongoDB and tags senders. |
| `projects/ms_block_analyser` | Celery worker that fans out each block to downstream queues (`indexers`, `events_and_impacted`, `contract`, `module_deployed`, etc.). |
| `projects/ms_accounts` | Consumer for the `account_creation` queue that enriches and stores every new address in `all_account_addresses`. |
| `projects/ms_events_and_impacts` | Processes each transaction to persist logged events plus all impacted accounts/contracts/public keys. |
| `projects/ms_indexers` | Builds Mongo indexes such as transfers, memoed transfers, contract touches, and module usage for fast querying. |
| `projects/ms_instances` | Tracks contract initialization/upgrades, enriches metadata via gRPC, and links contracts to known projects. |
| `projects/ms_modules` | Handles module deployments: fetches Wasm binaries, extracts entrypoints, runs Concordium-client verification, and updates module overviews. |
| `projects/ms_metadata` | Fetches CIS-2 metadata URLs discovered in token events and stores parsed token metadata + token-address state. |
| `projects/ms_plt` | Processes protocol-level token (PLT) transactions emitted by the chain so PLT stats stay current. |
| `projects/ms_token_accounting` | Maintains CIS token holdings by consuming logged events and updating `tokens_token_addresses_v2` / `tokens_links_v3`. |
| `projects/ms_bot_sender` | Dedicated Celery worker that actually delivers notification payloads to Telegram or email (used by the bot + notifier). |
| `projects/dagster_nightrunner` | Dagster code location for nightly analytics jobs such as realized prices, transactions-by-type/contents, and account graphs. |
| `projects/dagster_recurring` | Dagster code location for recurring maintenance jobs (memos, impacted address top lists, node stats, token-accounting backfills). |
| `projects/dagster_paydays` | Dagster code location focused on payday-related jobs; packaged for deployment with Dagster gRPC. |

### Components
| Path | Description |
| --- | --- |
| `components/ccdexplorer/grpc_client` | Concordium gRPC/protobuf definitions plus the typed client used across services. |
| `components/ccdexplorer/celery_app` | Central Celery application wiring, task result helpers, and worker bootstrap logic. |
| `components/ccdexplorer/domain` | Domain models for Concordium accounts, credentials, CIS events, Mongo document types, etc. |
| `components/ccdexplorer/mongodb` | MongoDB client wrappers (`MongoDB`, `MongoMotor`) and typed collection helpers. |
| `components/ccdexplorer/env` | Settings loader that centralizes environment variables (node endpoints, Redis, API keys, etc.). |
| `components/ccdexplorer/concordium_client` | Thin wrapper around `concordium-client` CLI for module verification and node diagnostics. |
| `components/ccdexplorer/cis` | Parser/executor for CIS-2/3/5 contracts that decodes logged events and token metadata. |
| `components/ccdexplorer/cns` | Concordium Name Service helper that interprets CNS events/actions and maps domain ownership. |
| `components/ccdexplorer/ccdscan` | Small GraphQL client for CCDScan so we can cross-check node data (blocks, baker stats, delegators). |
| `components/ccdexplorer/schema_parser` | Re-export of the schema parser used to interpret smart-contract schemas. |
| `components/ccdexplorer/site_user` | Pydantic models for explorer users, linked accounts, and notification preferences. |
| `components/ccdexplorer/tooter` | Notification transport hub (Telegram relays, Fastmail emails) plus enums for notifier channels. |
| `components/ccdexplorer/wasm_decoder` | WebAssembly parsing helpers (wadze) leveraged by module verification and metadata extraction. |

See the [documentation](https://docs.ccdexplorer.io) for a deeper dive into each brick and service.

## Getting started
1. **Install dependencies**
   ```zsh
   uv sync
   ```
2. **Configure environment** – copy `.env.sample` to `.env` and set a Concordium node endpoint and CCDExplorer API key (keys are domain-scoped, so match the API base you target).
3. **Run the explorer site**
   ```zsh
   uvicorn projects.site.asgi:app --loop asyncio --host 0.0.0.0 --port 8000
   ```
4. **Run the public API**
   ```zsh
   uvicorn projects.api.asgi:app --loop asyncio --port 7000
   ```
5. **Execute tests & formatting**
   ```zsh
   just test      # pytest with coverage
   just lint      # ruff linting
   just format    # ruff formatting
   ```

A Dockerfile is also provided if you prefer containerized builds of the site or API.

## Documentation
The full architecture, component glossary, and deployment details live at [docs.ccdexplorer.io](https://docs.ccdexplorer.io). The documentation mirrors the Polylith structure and links to project-specific guides (site, API, bot, timed services, and every-block workers).

## Contributing
Issues and pull requests are welcome. Please open a discussion for larger changes so we can ensure compatibility with the production explorer and downstream services. When contributing:
- keep bricks reusable across projects,
- add tests alongside new functionality,
- run `just lint` and `just test` before submitting.

Thanks for helping grow the Concordium ecosystem!
