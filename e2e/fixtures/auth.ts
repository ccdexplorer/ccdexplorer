import fs from 'fs';
import path from 'path';
import { test } from '@playwright/test';

/**
 * Written by global-setup.ts (a real login against the running site, saved as
 * Playwright storageState) when PLAYWRIGHT_TEST_USER_EMAIL/PASSWORD are set
 * in the repo-root .env. There's no way to reach a logged-in state otherwise
 * -- every account-creation flow (register, Telegram) ends in an email
 * verification step with no test bypass, so this relies on a pre-existing,
 * already-verified test account rather than creating one on the fly.
 */
export const AUTH_FILE = path.join(__dirname, '..', '.auth', 'user.json');

/**
 * Call at the top of a `test.describe` body (not inside a `test()` callback)
 * for specs that need a logged-in session. Applies the saved storageState and
 * skips the whole block -- rather than failing with a confusing missing-file
 * error -- when global-setup didn't produce one.
 */
export function useAuthenticatedUser() {
  test.use({ storageState: AUTH_FILE });
  test.skip(
    !fs.existsSync(AUTH_FILE),
    'requires PLAYWRIGHT_TEST_USER_EMAIL / PLAYWRIGHT_TEST_USER_PASSWORD in the repo-root .env',
  );
}
