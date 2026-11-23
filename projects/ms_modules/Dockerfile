# ---------------------------------------------------------
# Stage 1: bring in prebuilt Concordium tooling (cargo + client)
# ---------------------------------------------------------
FROM sderuiter/concordium-tools:8.0.0 AS concordium

# The docker client, for `cargo concordium verify-build`, which runs the
# module's build image through the host's socket. Same major as the host.
FROM docker:29.1.3-cli AS dockercli

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
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker

# cargo-concordium 5 verifies modules built for wasm32v1-none; the 4.x in the
# tools image verifies the ones built before. Their build images are not
# interchangeable, so both stay and the module's build image picks one
# (cargo_concordium_for in the subscriber).
ADD --chmod=755 \
    --checksum=sha256:2c35679ba33023a838feaca05d41e6fce8dcb6d7ca48ed7c60a283f832fa94ab \
    https://github.com/Concordium/concordium-smart-contract-tools/releases/download/releases%2Fcargo-concordium%2F5.0.0/cargo-concordium-x86_64-unknown-linux-gnu \
    /usr/local/bin/cargo-concordium-5

# --- OS dependencies (minimal runtime) ---
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
