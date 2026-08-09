import { test, expect } from '@playwright/test';
import { TX, NET } from '../fixtures/ids';

test('transaction detail page renders the correct hash and outcome', async ({ page }) => {
  const response = await page.goto(`/${NET}/transaction/${TX.hash}`);
  expect(response?.ok()).toBeTruthy();

  // tx/tx.html sets an og:title meta tag containing the tx hash.
  await expect(page.locator('meta[property="og:title"]')).toHaveAttribute(
    'content',
    new RegExp(TX.hash),
  );
  // tx/tx_header.html renders "Success" or "Rejected" for the outcome.
  await expect(page.getByText(/^(Success|Rejected)$/)).toBeVisible();
});
