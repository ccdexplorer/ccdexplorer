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

# How many test workers. Deliberately not `-n auto`, which asks for one per
# core and leaves the suite no headroom: anything else running on the machine
# -- a backup, a Spotlight reindex, a photo library pass -- then starves the
# workers, and a starved worker blows pytest-timeout's 150s ceiling on a test
# that normally takes 0.2s. That arrives as a failing test name, which is a
# lie: the test is fine and the machine is busy.
#
# Measured on an 8-core laptop with a backup running: `-n auto` took 739s and
# failed two tests, `-n 4` took 162s clean, `-n 3` took 155s clean. Half the
# cores is the default because it is the one that holds up when the machine
# is not idle.
#
# Override for a quiet machine or a bigger one: `just workers=12 test-fast`.
workers := "4"

test:
	uv run pytest -n {{workers}}

# Everything that does not need a Concordium node. This is what the pre-commit
# hook runs: the live tests depend on a remote host, and one of them has taken
# 220s inside a full run while taking 2s on its own.
test-fast:
	uv run pytest -n {{workers}} -m "not live"

# Only the tests that reach a real node.
test-live:
	uv run pytest -n {{workers}} -m live

test-coverage:
	uv run pytest --cov=. --cov-report=xml:cov.xml --cov-report=term -n {{workers}} && \
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

# Telegram fetches chart images by url from its own servers, so a site on
# 127.0.0.1 gives a bot whose every answer is a broken image. The quick
# tunnel is what makes the local site reachable.

# Chart bot against the local site, over a Cloudflare quick tunnel
bot-tunnel:
    ./scripts/bot-tunnel.sh

# Playwright end-to-end tests for ccdexplorer_site (headless)
e2e:
    cd e2e && npm test

accounts:
    python -m bases.ccdexplorer.accounts_retrieval

# Redraw the charts index covers (greyscale pngs, one per category)
chart-covers:
    uv run python bases/ccdexplorer/ccdexplorer_site/covers/make_chart_covers.py

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

# --- Site tooling ---

# Refresh the vendored Lottie website tester from upstream
# (github.com/asweigart/lottie-website-tester). The file is committed so it
# ships in the image; run this to pull a newer version, then commit the result.
lottie-update:
    mkdir -p projects/ccdexplorer_site/tools/lottie
    curl -fsSL -o projects/ccdexplorer_site/tools/lottie/lottie.html \
        https://raw.githubusercontent.com/asweigart/lottie-website-tester/main/lottie.html
    @echo "Fetched $(wc -c < projects/ccdexplorer_site/tools/lottie/lottie.html) bytes to projects/ccdexplorer_site/tools/lottie/lottie.html"
