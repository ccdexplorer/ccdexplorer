FROM ghcr.io/astral-sh/uv:python3.12-bookworm

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/app

WORKDIR /app

# 1) Copy workspace metadata first for caching
COPY pyproject.toml uv.lock README.md LICENSE.md ./

# System deps (psycopg2 build, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# 2) Copy the Polylith workspace sources (these must exist before uv sync if you have path deps)
COPY components ./components
COPY bases ./bases
COPY projects/dagster_recurring ./projects/dagster_recurring

# If your repo has other required workspace files (uncomment if they exist):
# COPY workspace.toml ./workspace.toml
# COPY polylith.toml ./polylith.toml
# COPY .polylith ./\.polylith

# 3) Sync env INCLUDING local path deps (Polylith bricks)
RUN mkdir -p /root/.cache/uv && chmod -R 777 /root/.cache/uv \
 && echo "==> uv sync (no dev)" \
 && uv sync --no-dev -vv

# 4) Ensure the code location project is installed (editable) so Dagster can import it
RUN echo "==> installing dagster_recurring project (editable)" \
 && uv pip install -e ./projects/dagster_recurring

# 5) Build-time sanity checks (fail fast if packaging/imports are wrong)
RUN python - <<'PY'
import importlib
m = importlib.import_module("ccdexplorer.dagster_recurring.repository")
print("OK imported:", m.__name__)
PY

# 6) Dagster home + user
RUN mkdir -p $DAGSTER_HOME \
 && addgroup --system app \
 && adduser --system --ingroup app app \
 && chown -R app:app $DAGSTER_HOME

USER app
EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "ccdexplorer.dagster_recurring.repository", "-a", "defs"]