import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { PROJECT } from '../fixtures/ids';

test('project detail page renders', async ({ page }) => {
  const response = await page.goto(`/project/${PROJECT.id}`);
  await expectPageRendered(page, response);
});
