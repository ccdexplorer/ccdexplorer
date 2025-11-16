# Dagster Paydays project

This project packages the `ccdexplorer.dagster_paydays` base into a deployable Dagster code
location so we can keep the Concordium payday analytics up to date.

## Runtime layout
`projects/dagster_paydays/pyproject.toml` installs the base plus every brick it depends on:

- Dagster runtime extras (`dagster`, `dagster-postgres`, `dagster-docker`) so the code location
  can run inside the orchestrator cluster.
- Shared CCDExplorer bricks such as `ccdexplorer.grpc_client`, `ccdexplorer.mongodb`, and
  `ccdexplorer.tooter` for on-chain data access, persistence, and notifications.
- The broader analytics stack (BetterProto, PyMongo, Redis, Celery) that the underlying payday
  routines call.

When you build the Docker image defined in `projects/dagster_paydays/Dockerfile`, the image:

1. Installs `uv`, syncs all dependencies, and copies the repo contents that the base needs.
2. Installs the project in editable mode so Dagster can import the `ccdexplorer.dagster_paydays`
   package.
3. Configures `$DAGSTER_HOME`, exposes port 4000, and launches `dagster api grpc -m
   ccdexplorer.dagster_paydays.repository -a defs` so the orchestrator can load the definitions
   over gRPC.

## Local development tips
- Use `uv pip install -e ./projects/dagster_paydays` while inside the repo to get the same
  editable layout as the container.
- Point your Dagster instance at `ccdexplorer.dagster_paydays.repository:defs` to load the
  payday assets locally.
- Configure the environment variables consumed by `ccdexplorer.env.settings` so the MongoDB and
  gRPC clients can connect to your Concordium data sources.
