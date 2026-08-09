import { expect, Page, Response } from '@playwright/test';

/**
 * Some site routes catch errors and still return 200 with
 * base/error.html (a ".bg-danger-subtle" banner) instead of a non-2xx
 * status, so checking response.ok() alone isn't enough to catch a broken
 * page. Use this after page.goto() for a lightweight "did this page
 * actually render" smoke check.
 */
export async function expectPageRendered(page: Page, response: Response | null) {
  expect(response?.ok()).toBeTruthy();
  await expect(page.locator('.bg-danger-subtle')).toHaveCount(0);
}
