import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET } from '../fixtures/ids';

// tools.py: the "Tools" nav dropdown, mainnet-only.
for (const path of [
  `/${NET}/tools/business-accounts`,
  `/${NET}/tools/chain-information`,
  `/${NET}/tools/labeled-accounts`,
  `/${NET}/tools/validators-failed-rounds`,
  `/${NET}/tools/transactions-search`,
  `/${NET}/tools/projects`,
  `/${NET}/today-in`,
  `/${NET}/transactions-by-type`,
  `/${NET}/accounts-scheduled-release`,
  `/${NET}/accounts-cooldown`,
  '/mainnet/tools/exchange-rates',
]) {
  test(`${path} renders`, async ({ page }) => {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  });
}
