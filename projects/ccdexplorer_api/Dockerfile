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
COPY development/grpc_gen ./development/grpc_gen
COPY projects/ccdexplorer_api ./projects/ccdexplorer_api

RUN uv pip install -e ./projects/ccdexplorer_api

# Add entrypoint to root of image
COPY projects/ccdexplorer_api/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


RUN addgroup --system app && adduser --system --ingroup app app

USER app

WORKDIR /app/projects/ccdexplorer_api

# Last, and deliberately so: an ARG that changes on every commit invalidates
# every layer built after it, so declaring this near the top would defeat the
# build cache on each push. Nothing below it is expensive.
#
# Baked in here rather than set in the compose file because the blue/green
# deploy gate asks a running container which build it is, and an answer the
# deployer supplied would only repeat what the deployer already believed.
ARG BUILD_TAG=unknown
ENV BUILD_TAG=${BUILD_TAG}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "asgi:app", "--host", "0.0.0.0", "--port", "80", "--no-access-log", "--workers", "2"]
