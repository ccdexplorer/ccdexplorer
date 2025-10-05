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
    
# --- Formatting / linting / testing ---

lint:
    uv run ruff check .

format:
    uv run ruff format .

test:
    uv run pytest  --cov=. --cov-report=xml:cov.xml --cov-report=term -n auto \
    uv run coverage html \
    open htmlcov/index.html


 
# --- Container helpers (optional) ---

build:
    docker build -t ccdexplorer:latest .

run:
    docker run --rm -it ccdexplorer:latest

api:
    uvicorn projects.ccdexplorer_api.asgi:app --loop asyncio --port 7000
# --- Help message ---

help:
    @echo "\nAvailable Just recipes:\n"
    @just --summary