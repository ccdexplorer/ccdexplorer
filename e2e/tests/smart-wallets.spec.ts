import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET, WALLET } from '../fixtures/ids';

test('smart wallets overview page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/smart-wallets`);
  await expectPageRendered(page, response);
});

test('smart wallet detail page renders', async ({ page }) => {
  const response = await page.goto(
    `/${NET}/smart-wallet/${WALLET.index}/${WALLET.subindex}/${WALLET.publicKey}`,
  );
  await expectPageRendered(page, response);
});
