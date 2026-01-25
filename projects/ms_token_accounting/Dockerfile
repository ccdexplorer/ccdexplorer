FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Install uv and base deps (from root lock, if you keep one)
COPY pyproject.toml uv.lock README.md LICENSE.md ./
RUN pip install --no-cache-dir uv \
 && uv sync --frozen --no-dev

# 2) Copy monorepo sources so the project can assemble bricks
COPY components ./components
COPY bases ./bases
COPY projects/ms_token_accounting ./projects/ms_token_accounting

# 3) Install the project package (this runs hatchling + hatch-polylith-bricks)
#    This is the crucial step that makes `ccdexplorer.*` importable.
RUN uv pip install -e ./projects/ms_token_accounting

# Create a non-root user and group
RUN addgroup --system app && adduser --system --ingroup app app

USER app

CMD ["python", "-m", "ccdexplorer.ms_token_accounting"]
#bump
