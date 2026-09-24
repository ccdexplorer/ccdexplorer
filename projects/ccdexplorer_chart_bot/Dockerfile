FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE.md ./
RUN pip install --no-cache-dir uv \
 && uv sync --frozen --no-dev

COPY components ./components
COPY bases ./bases
COPY projects/ccdexplorer_chart_bot ./projects/ccdexplorer_chart_bot

RUN uv pip install -e ./projects/ccdexplorer_chart_bot

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "-m", "ccdexplorer.ccdexplorer_chart_bot"]
