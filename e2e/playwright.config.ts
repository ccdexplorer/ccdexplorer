import { defineConfig, devices } from '@playwright/test';

const isCI = !!process.env.CI;

// Deliberately not 7000/8000: macOS's built-in AirPlay Receiver (AirTunes)
// squats port 7000 by default, and 8000 is a common Docker/OrbStack default
// too — both collide silently (the port "responds", so Playwright's
// webServer readiness check passes against the wrong process, and the real
// server never starts). Override via env if these also clash for you.
const apiPort = process.env.PLAYWRIGHT_API_PORT ?? '8010';
const sitePort = process.env.PLAYWRIGHT_SITE_PORT ?? '8011';

export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  retries: isCI ? 2 : 0,
  reporter: 'html',
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${sitePort}`,
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  // Starts both backing FastAPI processes so `npx playwright test` is a single
  // entry point. Both still need a real MONGO_URI / GRPC_* / CCDEXPLORER_API_KEY
  // (see components/ccdexplorer/env/settings.py) — there's no docker-compose or
  // mock backend in this repo, so a repo-root .env is required either way, same
  // as running `just api` / `just site` by hand. reuseExistingServer means these
  // are skipped if you already have them running locally on these ports.
  webServer: [
    {
      command: `uv run uvicorn projects.ccdexplorer_api.asgi:app --loop asyncio --port ${apiPort}`,
      cwd: '../',
      url: `http://127.0.0.1:${apiPort}/openapi.json`,
      reuseExistingServer: !isCI,
      timeout: 60_000,
    },
    {
      command: `uv run uvicorn projects.ccdexplorer_site.asgi:app --loop asyncio --port ${sitePort}`,
      cwd: '../',
      url: `http://127.0.0.1:${sitePort}/health`,
      reuseExistingServer: !isCI,
      timeout: 60_000,
      env: {
        // Spread first: Playwright's webServer `env` replaces process.env
        // rather than merging into it, so without this the child process
        // would lose MONGO_URI / GRPC_* / CCDEXPLORER_API_KEY etc.
        ...(process.env as Record<string, string>),
        // Point the locally-started site at the locally-started API rather
        // than whatever remote API_URL the dev's own .env might set.
        API_URL: `http://127.0.0.1:${apiPort}`,
      },
    },
  ],
});
