import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET } from '../fixtures/ids';

// statistics.py: mainnet-only nav item ("Statistics"). Subpages share the
// same statistics-chain*.html family of templates.
for (const path of [
  `/${NET}/statistics`,
  `/${NET}/statistics/accounts`,
  `/${NET}/statistics/chain`,
  `/${NET}/statistics/staking`,
  `/${NET}/statistics/validators`,
  `/${NET}/statistics/exchanges`,
]) {
  test(`${path} renders`, async ({ page }) => {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  });
}
