import { test, expect } from '@playwright/test';
import { ACCOUNT, NET } from '../fixtures/ids';

/**
 * Bootstrap 5.3's .table sets --bs-table-color to --bs-emphasis-color, which
 * custom_bootstrap.css resolves to #fff in dark and #000 in light. Neither is
 * the colour this site reads its tables in, so every one of 983 cells across
 * 93 templates carried a text-secondary-emphasis class purely to drag the
 * colour back -- the !important on that utility being the only thing beating
 * the .table rule.
 *
 * The fix is one declaration on .table in custom_ccdexplorer.css. These tests
 * strip the class at runtime and assert the cell is unchanged: that is the
 * whole claim, and it is what makes deleting the class from the templates a
 * no-op rather than a redesign.
 */

const SOFT = {
  // --bs-secondary-text-emphasis, per theme.
  dark: 'rgb(167, 172, 177)',
  light: 'rgb(43, 47, 50)',
};

for (const theme of ['dark', 'light'] as const) {
  test(`a table cell reads ${SOFT[theme]} in ${theme} without a class saying so`, async ({
    page,
  }) => {
    await page.addInitScript((t) => localStorage.setItem('bsTheme', t), theme);
    await page.goto(`/${NET}/account/${ACCOUNT.address}`);
    await expect(page.locator('html')).toHaveAttribute('data-bs-theme', theme);

    const cell = page.locator('table.table td').first();
    await expect(cell).toBeVisible();

    const colour = await cell.evaluate((td) => {
      td.classList.remove('text-secondary-emphasis');
      return getComputedStyle(td).color;
    });

    expect(colour).toBe(SOFT[theme]);
  });
}
