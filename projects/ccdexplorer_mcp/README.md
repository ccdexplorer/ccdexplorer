# CCDExplorer MCP

Standalone MCP service for AI clients such as Codex and other MCP-compatible tools.

The service exposes Streamable HTTP at `/mcp` using FastMCP. Its tools call the
existing CCDExplorer API over HTTP, so red/blue API port switching stays behind
NGINX and does not need to be duplicated in this service.

## Environment

- `API_CODEX_KEY` or `CCDEXPLORER_API_KEY`: API key used by MCP tools when calling
  the CCDExplorer API.
- `CCDEXPLORER_API_BASE_URL`: stable API base URL. Defaults to
  `https://api.ccdexplorer.io`.
- `CCDEXPLORER_MCP_AUTH_TOKEN`: optional token accepted by `/mcp` as either
  `Authorization: Bearer <token>` or `x-ccdexplorer-key: <token>`. Defaults to the
  API key above.
- `CCDEXPLORER_MCP_REQUEST_TIMEOUT`: outbound API request timeout in seconds.
  Defaults to `20`.

## Routes

- `GET /health`: unauthenticated health check.
- `/mcp`: MCP Streamable HTTP endpoint.
