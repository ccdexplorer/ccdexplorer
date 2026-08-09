import { test, expect } from '@playwright/test';
import { BLOCK, TX } from '../fixtures/ids';

// templates/base/search_bar.html: the magnifying-glass image has an explicit
// hx-trigger="click" (the text input itself only triggers on blur/change),
// so tests click that button rather than pressing Enter in the input.
const clickSearch = async (page: import('@playwright/test').Page) => {
  await page.locator('img[alt="searc_btn"]').click();
};

test('searching a block height navigates to the block page', async ({ page }) => {
  await page.goto('/mainnet');
  await page.selectOption('#search_selector', 'block');
  await page.fill('#search_value', String(BLOCK.height));
  await clickSearch(page);
  await page.waitForURL(`**/mainnet/block/${BLOCK.height}`);
});

test('searching a transaction hash navigates to the transaction page', async ({ page }) => {
  await page.goto('/mainnet');
  await page.selectOption('#search_selector', 'transaction');
  await page.fill('#search_value', TX.hash);
  await clickSearch(page);
  await page.waitForURL(`**/mainnet/transaction/${TX.hash}`);
});

test('changing the search selector updates the input placeholder', async ({ page }) => {
  await page.goto('/mainnet');
  const before = await page.locator('#search_value').getAttribute('placeholder');

  await page.selectOption('#search_selector', 'account');
  // search_bar.html fetches /search_placeholder on selector change.
  await expect
    .poll(async () => page.locator('#search_value').getAttribute('placeholder'))
    .not.toBe(before);
});
