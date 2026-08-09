import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET, NODE } from '../fixtures/ids';

test('nodes overview page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/nodes`);
  await expectPageRendered(page, response);
});

test('node detail page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/node/${NODE.id}`);
  await expectPageRendered(page, response);
});
