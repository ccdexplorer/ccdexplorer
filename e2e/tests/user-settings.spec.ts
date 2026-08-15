import { test, expect } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { useAuthenticatedUser } from '../fixtures/auth';

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

// templates/userv2/user_settings_all.html: the logged-in view is a Bootstrap
// nav-pills/tab-content page (same pattern as the account detail page), with
// User Settings selected by default and Account Security, General, Account,
// Contract as the other pills. All panes render server-side up front (no
// HTMX/AJAX tab loading here, unlike the account page), so switching pills is
// a pure client-side Bootstrap Tab show/hide.
test.describe('overview page pills (logged in)', () => {
  useAuthenticatedUser();

  test('User Settings pill is selected by default, others are hidden', async ({ page }) => {
    const response = await page.goto('/settings/user/overview');
    await expectPageRendered(page, response);

    const pills = page.locator('#settings-pills-tab .nav-link');
    await expect(pills).toHaveText([
      'User Settings',
      'Account Security',
      'General',
      'Account',
      'Contract',
    ]);
    await expect(page.locator('#user-settings-tab')).toHaveClass(/active/);
    await expect(page.locator('#user-settings')).toBeVisible();
    await expect(page.locator('#account-security')).toBeHidden();
    await expect(page.locator('#general-notifications')).toBeHidden();
    await expect(page.locator('#account-notifications')).toBeHidden();
    await expect(page.locator('#contract-notifications')).toBeHidden();
  });

  test('clicking a pill switches the visible pane', async ({ page }) => {
    await page.goto('/settings/user/overview');

    await page.locator('#account-security-tab').click();
    await expect(page.locator('#account-security')).toBeVisible();
    await expect(page.locator('#user-settings')).toBeHidden();
    // The Account Security pane's own content (added alongside session
    // rotation/reuse-detection) should be there.
    await expect(page.getByText('Active sessions')).toBeVisible();

    await page.locator('#general-notifications-tab').click();
    await expect(page.locator('#general-notifications')).toBeVisible();
    await expect(page.locator('#account-security')).toBeHidden();
  });

  test('the /.well-known/change-password redirect selects the Account Security pill', async ({
    page,
  }) => {
    // factory.py's well_known_change_password redirects here with a hash;
    // user_settings_all.html has an inline script that activates the
    // matching pill on load since Bootstrap doesn't do that for a bare
    // #anchor by itself.
    await page.goto('/.well-known/change-password');
    await expect(page).toHaveURL(/\/settings\/user\/overview#account-security$/);
    await expect(page.locator('#account-security-tab')).toHaveClass(/active/);
    await expect(page.locator('#account-security')).toBeVisible();
  });
});
