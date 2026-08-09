import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { CONTRACT, NET, TOKEN } from '../fixtures/ids';

test('tokens listing page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/tokens`);
  await expectPageRendered(page, response);
});

test('token detail page renders', async ({ page }) => {
  const response = await page.goto(
    `/${NET}/token/${CONTRACT.index}/${CONTRACT.subindex}/${TOKEN.id}`,
  );
  await expectPageRendered(page, response);
});
