import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET } from '../fixtures/ids';

// staking.py: mainnet-only nav item; templates/staking/staking_tabs.html
// (Paydays / Staking Pools pills).
test('staking page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/staking`);
  await expectPageRendered(page, response);
});
