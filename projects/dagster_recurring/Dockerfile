FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE.md ./

RUN apt-get update && apt-get install -y gcc libpq-dev \
 && pip install --no-cache-dir --upgrade pip setuptools wheel hatchling uv \
 && mkdir -p /root/.cache/uv && chmod -R 777 /root/.cache/uv \
 && test -f pyproject.toml || (echo "pyproject.toml missing in $(pwd)" && exit 1) \
 && echo "==> Running uv sync..." \
 && UV_SYSTEM_PYTHON=1 uv sync --no-dev -vv

# 2) Copy all sources required to assemble the Dagster project
COPY components ./components
COPY bases ./bases
COPY development/grpc_gen ./development/grpc_gen
COPY projects/dagster_recurring ./projects/dagster_recurring

# 3) Install the Dagster project as editable (important for brick discovery)
RUN uv pip install -e ./projects/dagster_recurring

# 4) Create Dagster home directory and logs subfolder
RUN mkdir -p $DAGSTER_HOME/logs

# 5) Create non-root user with a fixed, safe UID/GID (e.g., 1000)
RUN addgroup -g 1000 -S app && adduser -u 1000 -S -G app app

# 6) Force ownership using the explicit numeric IDs
RUN chown -R 1000:1000 /opt/dagster/dagster_home

# 7) Switch to user
USER app

# 8) Expose gRPC port for Dagster
EXPOSE 4000

# 9) Launch Dagster code location gRPC server
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "ccdexplorer.dagster_recurring.repository", "-a", "defs"]
