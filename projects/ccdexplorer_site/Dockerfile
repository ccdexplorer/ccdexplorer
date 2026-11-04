FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_SYSTEM_PYTHON=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BROWSER_PATH=/usr/bin/chromium

WORKDIR /app

# Install Chromium (for Kaleido) + headless runtime deps
RUN apt-get update && apt-get install -y \
    chromium \
 && rm -rf /var/lib/apt/lists/*

# 1) Install deps
COPY pyproject.toml uv.lock README.md LICENSE.md ./
RUN uv sync --frozen --no-dev

# 2) Copy monorepo sources so the project can assemble bricks
COPY components ./components
COPY bases ./bases
COPY development/grpc_gen ./development/grpc_gen
COPY projects/ccdexplorer_site ./projects/ccdexplorer_site

# 3) Install the project package (this runs hatchling + hatch-polylith-bricks)
RUN uv pip install -e ./projects/ccdexplorer_site

# Add entrypoint to root of image
COPY projects/ccdexplorer_site/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


# Create a non-root user with a real home directory
RUN addgroup --system app \
 && adduser  --system --ingroup app --home /home/app --shell /bin/bash app \
 && mkdir -p /home/app \
 && chown -R app:app /home/app

# Make sure Chrome has writable dirs
ENV HOME=/home/app \
    XDG_CACHE_HOME=/home/app/.cache \
    XDG_CONFIG_HOME=/home/app/.config \
    XDG_DATA_HOME=/home/app/.local/share

USER app

WORKDIR /app/projects/ccdexplorer_site
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
CMD ["uvicorn", "asgi:app", "--host", "0.0.0.0", "--port", "80", "--no-access-log"]
