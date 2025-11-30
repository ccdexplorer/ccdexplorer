# Notification Bot (Project: `ccdexplorer_bot`)

The Telegram/email notification bot lets users subscribe to Concordium events (account transfers, validator changes, token events, etc.). It has been rewritten multiple times to keep up with the expanding notification catalogue.

### Install and Run
0. Git clone, make a venv (`python3 -m venv .venv`) and activate it. 
1. Install dependencies (in the venv)
```zsh
uv sync
```
2. Set ENV variables
Copy the `.env.sample` to `.env`. Fill in the `API_TOKEN` with your Telegram bot token. 

### Run Tests
All notification types should have a corresponding test. 
Use `pytest` to test these. 

## Deployment
A Dockerfile is supplied that builds the project into a Docker image (this is the image that is being used on the CCDExplorer Bot).

## Relation to other services
- Emits notification requests consumed by [`ms_bot_sender`](bot_sender.md).
- Reads user preferences and aliases defined in the [`site_user`](../components/site-user.md) models.
- Pulls chain data from the API plus Redis-backed Celery tasks (e.g. validator heartbeat status).***
