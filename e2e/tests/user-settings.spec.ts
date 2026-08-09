import { test, expect } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';

// usersv2.py::user_settings_all: renders fine logged-out (user=None), which
// is what the navbar's "Login" link (shown when there's no session) points
// at — see templates/base/navbar.html.
test('user settings overview renders when logged out', async ({ page }) => {
  const response = await page.goto('/settings/user/overview');
  await expectPageRendered(page, response);
});

test('navbar Login link points at the settings overview page', async ({ page }) => {
  await page.goto('/mainnet');
  // Like the "Blockchain" dropdown toggle, this <a> has an explicit
  // role="button" override, so it isn't accessible-role "link".
  await expect(page.locator('a.nav-link', { hasText: 'Login' })).toHaveAttribute(
    'href',
    '/settings/user/overview',
  );
});
