import { test, expect } from '@playwright/test';

// auth.py::login_post re-renders auth/login.html with a `text-danger` error
// paragraph on failure (200, no redirect) rather than an HTMX partial swap,
// so this is a plain form submission. The message text comes from the
// upstream API's error `detail` (or a generic fallback), so we assert it's
// present rather than asserting exact wording.
test('logging in with invalid credentials re-renders the login page with an error', async ({
  page,
}) => {
  await page.goto('/auth/login');
  await page.fill('input[name="email"]', 'definitely-not-a-real-user@example.com');
  await page.fill('input[name="password"]', 'wrong-password');
  await page.click('button[type="submit"]');

  await expect(page).toHaveURL(/\/auth\/login$/);
  await expect(page.locator('p.text-danger')).toBeVisible();
});

test('login form requires both fields', async ({ page }) => {
  await page.goto('/auth/login');
  await expect(page.locator('input[name="email"]')).toHaveAttribute('required', '');
  await expect(page.locator('input[name="password"]')).toHaveAttribute('required', '');
});
