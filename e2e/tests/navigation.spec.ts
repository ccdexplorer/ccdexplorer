import { test, expect } from '@playwright/test';

// templates/base/navbar.html shows different "Blockchain" dropdown items
// depending on the active net.
test('mainnet-only nav items are hidden on devnet, and vice versa', async ({ page }) => {
  // The "Blockchain" toggle is an <a role="button"> (Bootstrap dropdown), not
  // a navigational link, so target it by text rather than the link role.
  const blockchainToggle = page.locator('a.dropdown-toggle', { hasText: 'Blockchain' });

  await page.goto('/mainnet');
  await blockchainToggle.click();
  await expect(page.locator('.dropdown-item', { hasText: 'Staking' })).toBeVisible();
  await expect(page.locator('.dropdown-item', { hasText: 'PLT Locks' })).toHaveCount(0);

  await page.goto('/devnet');
  await blockchainToggle.click();
  await expect(page.locator('.dropdown-item', { hasText: 'PLT Locks' })).toBeVisible();
  await expect(page.locator('.dropdown-item', { hasText: 'Staking' })).toHaveCount(0);
});

test('mobile viewport collapses the navbar behind a toggler', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto('/mainnet');

  const collapse = page.locator('#navbarSupportedContent');
  await expect(collapse).not.toBeVisible();

  await page.locator('.navbar-toggler').click();
  await expect(collapse).toBeVisible();
});
