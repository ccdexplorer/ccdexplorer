# ---------------------------------------------------------
# Stage 1: bring in prebuilt Concordium tooling (cargo + client)
# ---------------------------------------------------------
FROM sderuiter/concordium-tools:8.0.0 AS concordium

# ---------------------------------------------------------
# Stage 2: main Python runtime (uv-based Polylith project)
# ---------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy prebuilt Concordium binaries from the base layer
COPY --from=concordium /usr/local/bin/cargo* /usr/local/bin/
COPY --from=concordium /usr/local/bin/concordium-client /usr/local/bin/

# --- OS dependencies (minimal runtime + Docker client only) ---
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl  gosu; \
    rm -rf /var/lib/apt/lists/*

# --- Install uv deps (workspace frozen lock) ---
COPY pyproject.toml uv.lock README.md LICENSE.md ./
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev

# --- Copy Polylith bricks and the ms_modules project ---
COPY components ./components
COPY bases ./bases
COPY development/grpc_gen ./development/grpc_gen
COPY projects/ms_modules ./projects/ms_modules

# Install the project editably (hatch-polylith assembles it)
RUN uv pip install -e ./projects/ms_modules

# --- Runtime user and dirs ---
RUN addgroup --system app && adduser --system --ingroup app app
RUN mkdir -p /app/tmp /app/bin && chmod 777 /app/tmp

# --- Entrypoint ---
COPY projects/ms_modules/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER root

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "ccdexplorer.ms_modules"]
