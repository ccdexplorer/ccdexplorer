import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';

// home.py: static informational pages, no net-scoping.
for (const path of ['/misc/release-notes', '/misc/privacy-policy', '/misc/support']) {
  test(`${path} renders`, async ({ page }) => {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  });
}
