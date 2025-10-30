The bot. Rewritten three times. 

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