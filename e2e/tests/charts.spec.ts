import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET } from '../fixtures/ids';

// charts/charts_home.py + individual chart routers. Only a couple of the
// underlying Plotly charts are checked here (they share the same rendering
// mechanism), rather than every chart route.
for (const path of [
  `/${NET}/charts`,
  `/${NET}/charts/holders`,
  `/${NET}/charts/active-addresses`,
]) {
  test(`${path} renders`, async ({ page }) => {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  });
}
