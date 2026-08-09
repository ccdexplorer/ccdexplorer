import { test, expect } from '@playwright/test';
import { ACCOUNT, NET } from '../fixtures/ids';

// The account's "Tokens" tab renders a Tabulator grid (#fungible_verified_table)
// configured via base/tabulator/tabulator-common-options-get.html with
// pagination:true, remote paging. No custom `paginationElement` div exists in
// the templates, so Tabulator renders its default paginator/footer inside the
// table's own container.
test('fungible tokens table paginates via the Tabulator "next" button', async ({ page }) => {
  await page.goto(`/${NET}/account/${ACCOUNT.address}`);
  await page.locator('#tokens-pills-tab').click();

  const table = page.locator('#fungible_verified_table');
  await expect(table.locator('.tabulator-row').first()).toBeVisible({ timeout: 10_000 });

  const nextButton = table.locator('.tabulator-page[data-page="next"]');
  test.skip(await nextButton.isDisabled(), 'account only has a single page of tokens');

  const firstRowBefore = await table.locator('.tabulator-row').first().innerText();
  await nextButton.click();
  await expect(table.locator('.tabulator-row').first()).not.toHaveText(firstRowBefore);
});
