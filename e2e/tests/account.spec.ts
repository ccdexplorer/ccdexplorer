import { test, expect } from '@playwright/test';
import { ACCOUNT, NET } from '../fixtures/ids';

// templates/account/account_account.html lazy-loads each pill tab's content
// via a plain fetch() into a "Loading..." placeholder div, keyed by tab id.
test('account page loads and the transactions tab replaces its Loading placeholder', async ({
  page,
}) => {
  const response = await page.goto(`/${NET}/account/${ACCOUNT.address}`);
  expect(response?.ok()).toBeTruthy();

  const txTabContent = page.locator('#transactions-tab-content');
  await expect(txTabContent).not.toHaveText('Loading...', { timeout: 10_000 });
});

test('switching to the tokens tab loads its content', async ({ page }) => {
  await page.goto(`/${NET}/account/${ACCOUNT.address}`);

  await page.locator('#tokens-pills-tab').click();
  await expect(page.locator('#pills-tokens')).toBeVisible();

  const tokensTabContent = page.locator('#tokens-tab-content');
  await expect(tokensTabContent).not.toHaveText('Loading...', { timeout: 10_000 });
});
