import { test, expect } from '@playwright/test';

test('home page renders for each net and the search bar is present', async ({ page }) => {
  for (const net of ['mainnet', 'testnet', 'devnet']) {
    await page.goto(`/${net}`);
    await expect(page.locator('#search-element')).toBeVisible();
    await expect(page.locator('#net_switcher')).toHaveValue(net);
  }
});

test('net switcher navigates to the selected net', async ({ page }) => {
  await page.goto('/mainnet');
  await page.selectOption('#net_switcher', 'testnet');
  await page.waitForURL('**/testnet');
  await expect(page.locator('#net_switcher')).toHaveValue('testnet');
});

test('latest-finalized-blocks widget polls and populates rows', async ({ page }) => {
  await page.goto('/mainnet');
  // home.html hx-gets /{net}/ajax_last_blocks on load and every 2s.
  const blocksWidget = page.locator('.row.home .col-lg-6').first();
  await expect(blocksWidget.getByText('Latest Finalized Blocks')).toBeVisible();
  await expect(blocksWidget.locator('tbody tr').first()).toBeVisible({ timeout: 10_000 });
});
