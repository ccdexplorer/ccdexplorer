FROM ghcr.io/astral-sh/uv:python3.12-bookworm

# ---- System setup ----
RUN apt-get update && apt-get install -y git gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# ---- Environment ----
ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /app

# ---- 1) Install uv and base deps ----
COPY pyproject.toml uv.lock README.md LICENSE.md ./
RUN pip install --no-cache-dir uv \
 && uv sync --no-dev

# ---- 2) Copy monorepo sources ----
COPY components ./components
COPY bases ./bases
COPY projects/accounts_retrieval ./projects/accounts_retrieval

# ---- 3) Install the project editably ----
RUN uv pip install -e ./projects/accounts_retrieval

# ---- 4) Clone the external repo ----
ARG CE_BOT_TOKEN
ENV CE_BOT_TOKEN=${CE_BOT_TOKEN}
RUN git config --global user.name "ceupdaterbot" \
 && git config --global user.email "bot@ccdexplorer.io" \
 && git clone https://ceupdaterbot:${CE_BOT_TOKEN}@github.com/ccdexplorer/ccdexplorer-accounts.git /home/git_dir \
 && git config --global --add safe.directory /home/git_dir

# ---- 5) Create non-root user ----
RUN addgroup --system app && adduser --system --ingroup app app \
 && chown -R app:app /home/git_dir
USER app

# ---- 6) Run script ----
CMD ["python", "-m", "ccdexplorer.accounts_retrieval"]