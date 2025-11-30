# CCDExplorer Site (Project: `ccdexplorer_site`)

The site is built on [FastAPI](https://fastapi.tiangolo.com/) with Jinja templates, and all data is retrieved via the Explorer API.

## Getting Started

### Prerequisites

1. An API Key. You can purchase a key on https://api.ccdexplorer.io or request a development API key on Telegram (DM me).

### Install and Run
0. Git clone, make a venv (`python3 -m venv .venv`) and activate it. 
1. Install dependencies (in the venv)
```zsh
uv sync
```
2. Set ENV variables
Copy the `.env.sample` to `.env`. Fill in the CCD-EXPLORER-KEY. Note that API keys are scoped to a domain, so an API key requested for api.ccdexplorer.io will NOT work on dev-api.ccdexplorer.io and vice-versa. Adjust your API_URL env variable accordingly.

3. Start FastAPI process
```zsh
uvicorn projects.site.asgi:app --loop asyncio --host 0.0.0.0 --port 8000
```
4. Open a browser window at [http://localhost:8000](http://localhost:8000).

## Deployment
A Dockerfile is supplied that builds the project into a Docker image (this is the image that is being used on [CCDExplorer.io](https://ccdexplorer.io)).

## Notable features
- Server-side rendered pages with progressive enhancement for live charts.
- Alias-aware account URLs powered by utilities in `bases/ccdexplorer/ccdexplorer_site/app/utils.py`.
- Integration with the notification stack so users can manage subscriptions directly from `/settings`.***
