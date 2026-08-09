import { test, expect } from '@playwright/test';
import { BLOCK, NET } from '../fixtures/ids';

test('block detail page renders the correct hash and height', async ({ page }) => {
  const response = await page.goto(`/${NET}/block/${BLOCK.height}`);
  expect(response?.ok()).toBeTruthy();

  // block/block.html sets an og:title meta tag containing height + hash;
  // block/block_header.html renders height in a #block_height span with a
  // title attribute holding the raw (unformatted) height value.
  await expect(page.locator('meta[property="og:title"]')).toHaveAttribute(
    'content',
    new RegExp(BLOCK.hash),
  );
  await expect(page.locator('#block_height')).toHaveAttribute('title', String(BLOCK.height));
});

test('looking up a block by hash resolves to the same block', async ({ page }) => {
  const response = await page.goto(`/${NET}/block/${BLOCK.hash}`);
  expect(response?.ok()).toBeTruthy();
  await expect(page.locator('#block_height')).toHaveAttribute('title', String(BLOCK.height));
});
