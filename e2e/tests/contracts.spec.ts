import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { CONTRACT, MODULE, NET } from '../fixtures/ids';

test('contract instance page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/contract/${CONTRACT.index}/${CONTRACT.subindex}`);
  await expectPageRendered(page, response);
});

test('module page renders', async ({ page }) => {
  const response = await page.goto(`/${NET}/module/${MODULE.ref}`);
  await expectPageRendered(page, response);
});

test('smart contracts overview pages render', async ({ page }) => {
  for (const path of [
    `/${NET}/tools/smart-contracts`,
    `/${NET}/tools/smart-contracts/all`,
  ]) {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  }
});
