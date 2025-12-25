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

# After: RUN uv pip install -e ./projects/dagster_recurring

RUN python - <<'PY'
import sys, pkgutil, importlib, os, pathlib

print("== sys.path ==")
for p in sys.path:
    print(" ", p)

print("\n== /app tree (top levels) ==")
p = pathlib.Path("/app")
for name in ["projects", "bases", "components", "src"]:
    pp = p / name
    print(name, "exists:", pp.exists())

print("\n== installed dists (filtered) ==")
try:
    import importlib.metadata as md
    dists = sorted([d.metadata["Name"] for d in md.distributions()])
    for n in dists:
        if "dagster" in n.lower() or "ccd" in n.lower() or "recurr" in n.lower() or "polyl" in n.lower():
            print(" ", n)
except Exception as e:
    print("metadata error:", e)

print("\n== visible top-level modules (filtered) ==")
mods = sorted([m.name for m in pkgutil.iter_modules() if any(k in m.name for k in ["ccd", "explorer", "recurr", "dagster_recurring"])])
for m in mods[:200]:
    print(" ", m)

print("\n== import attempts ==")
candidates = [
    "ccdexplorer.dagster_recurring.repository",
    "dagster_recurring.repository",
    "projects.dagster_recurring.repository",
]
for mod in candidates:
    try:
        importlib.import_module(mod)
        print(" OK:", mod)
    except Exception as e:
        print(" FAIL:", mod, "->", repr(e))

# If nothing worked, fail the build explicitly:
raise SystemExit(1)
PY


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