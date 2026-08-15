import fs from 'fs';
import path from 'path';
import { request, FullConfig } from '@playwright/test';
import dotenv from 'dotenv';
import { AUTH_FILE } from './fixtures/auth';

// The repo-root .env is what the API/site webServer processes already load
// themselves (see playwright.config.ts's webServer env spread) -- Node
// doesn't get that for free, so load it here too for
// PLAYWRIGHT_TEST_USER_EMAIL/PASSWORD.
dotenv.config({ path: path.join(__dirname, '..', '.env') });

export default async function globalSetup(config: FullConfig) {
  const email = process.env.PLAYWRIGHT_TEST_USER_EMAIL;
  const password = process.env.PLAYWRIGHT_TEST_USER_PASSWORD;
  if (!email || !password) {
    console.warn(
      '[global-setup] PLAYWRIGHT_TEST_USER_EMAIL / PLAYWRIGHT_TEST_USER_PASSWORD not set in ' +
        '.env -- skipping login. Specs using fixtures/auth.ts\'s useAuthenticatedUser() will ' +
        'skip themselves.',
    );
    return;
  }

  const baseURL = (config.projects[0].use as { baseURL?: string }).baseURL;
  const context = await request.newContext({ baseURL });
  try {
    const response = await context.post('/auth/login', { form: { email, password } });
    // auth.py::login_post always returns 200 on failure too -- it re-renders
    // auth/login.html with an error banner rather than a non-2xx status (see
    // auth.spec.ts) -- so response.ok() alone can't detect a failed login.
    // The only reliable signal is whether the login actually set the session
    // cookie.
    const gotSessionCookie = (await context.storageState()).cookies.some(
      (c) => c.name === 'access-token',
    );
    if (!response.ok() || !gotSessionCookie) {
      const body = await response.text();
      throw new Error(
        `[global-setup] Login failed for the e2e test account (status ${response.status()}, ` +
          `no access-token cookie set). Check PLAYWRIGHT_TEST_USER_EMAIL / ` +
          `PLAYWRIGHT_TEST_USER_PASSWORD in .env, and that the account exists in users_v2_dev ` +
          `and its email is verified. Response body: ${body.slice(0, 500)}`,
      );
    }
    fs.mkdirSync(path.dirname(AUTH_FILE), { recursive: true });
    await context.storageState({ path: AUTH_FILE });
  } finally {
    await context.dispose();
  }
}
