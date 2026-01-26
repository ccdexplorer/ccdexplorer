FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# ---- System setup ----
RUN apt-get update && apt-get install -y git gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# ---- Environment ----
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
# ---- 2) Copy source bricks ----
COPY components ./components
COPY bases ./bases
COPY projects/dagster_nightrunner ./projects/dagster_nightrunner

# ---- 3) Install project in editable mode ----
RUN uv pip install -e ./projects/dagster_nightrunner

# ---- 4) Dagster home ----


# ---- 5) Git setup for CE bot ----
# Clone target repo and configure Git identity
RUN git clone https://github.com/ccdexplorer/ccdexplorer-accounts.git /home/git_dir && \
    git config --global user.name "ceupdaterbot" && \
    git config --global user.email "bot@ccdexplorer.io"

# Configure authenticated HTTPS remote using build secret
# During docker build, pass:
#   --secret id=CE_BOT_TOKEN,env=CE_BOT_TOKEN
RUN --mount=type=secret,id=CE_BOT_TOKEN \
    git config --global url."https://ceupdaterbot:$(cat /run/secrets/CE_BOT_TOKEN)@github.com/".insteadOf "https://github.com/"

# ---- 6) Non-root runtime user ----
# 4) Create Dagster home early
RUN mkdir -p $DAGSTER_HOME

# 5) Create non-root user first
# 5) Create non-root user
RUN addgroup --system app && adduser --system --ingroup app app

# 6) Fix permissions (after user exists)
RUN chown -R app:app $DAGSTER_HOME /home/git_dir

# 7) Configure Git safe.directory (while still root)
RUN git config --system --add safe.directory /home/git_dir

# 8) Switch to user
USER app
# ---- 7) Expose Dagster gRPC port ----
EXPOSE 4000

# ---- 8) Start Dagster gRPC server ----
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "ccdexplorer.dagster_nightrunner.repository", "-a", "defs"]