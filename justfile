# ==============================
# CCDExplorer workspace commands
# ==============================

# default recipe
default := "info"

# --- Polylith commands ---

info:
    uv run poly info

libs:
    uv run poly libs

check:
    uv run poly check

deps:
    uv run poly deps

diff:
    uv run poly diff

# create new bricks: `just base my_name`
base NAME="":
    uv run poly create base --name {{NAME}}

component NAME="":
    uv run poly create component --name {{NAME}}

project NAME="":
    uv run poly create project --name {{NAME}}

# --- UV environment management ---

sync:
    uv sync --refresh

lock:
    uv lock

# mkdocs documentation
docs:
    uv run mkdocs serve -a 0.0.0.0:8001 --livereload
    
zens:
    uv run zensical serve -a 0.0.0.0:8001
# --- Formatting / linting / testing ---

lint:
    uv run ruff check .

format:
    uv run ruff format .

test:
	uv run pytest -n auto

test-coverage:
	uv run pytest --cov=. --cov-report=xml:cov.xml --cov-report=term -n auto && \
	uv run coverage html
	# open htmlcov/index.html


 
# --- Container helpers (optional) ---

build:
    docker build -t ccdexplorer:latest .

run:
    docker run --rm -it ccdexplorer:latest

api:
    uvicorn projects.ccdexplorer_api.asgi:app --loop asyncio --port 7000

site:
    uvicorn projects.ccdexplorer_site.asgi:app --reload --loop asyncio --port 8000

mcp:
    uvicorn projects.ccdexplorer_mcp.asgi:app --loop asyncio --port 8765

# Playwright end-to-end tests for ccdexplorer_site (headless)
e2e:
    cd e2e && npm test

accounts:
    python -m bases.ccdexplorer.accounts_retrieval

# --- Devnet ---

# Drop and rebuild the concordium_devnet database from scratch (asks for confirmation).
# Extra flags are passed through, e.g. `just rebuild-devnet --redis-url redis://localhost:6379/0`
rebuild-devnet *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "WARNING: this will DROP the concordium_devnet database and rebuild it from scratch."
    read -p "Are you sure you want to continue? [y/N] " confirm
    case "$confirm" in
        [yY][eE][sS]|[yY])
            uv run python scripts/rebuild_devnet.py {{ARGS}}
            ;;
        *)
            echo "Aborted."
            exit 1
            ;;
    esac

# Export/apply real MongoDB index definitions between networks (source of truth
# is the live database, not hand-maintained code).
# e.g. `just indices export --db concordium_mainnet --out mainnet_indices.json`
#      `just indices apply --db concordium_devnet --file mainnet_indices.json`
indices *ARGS:
    uv run python -m ccdexplorer.mongodb.index_migration {{ARGS}}

# --- Help message ---

help:
    @echo "\nAvailable Just recipes:\n"
    @just --summary