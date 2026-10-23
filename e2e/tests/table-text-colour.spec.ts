import { test, expect } from '@playwright/test';
import { ACCOUNT, NET } from '../fixtures/ids';

/**
 * Bootstrap 5.3's .table sets --bs-table-color to --bs-emphasis-color, which
 * custom_bootstrap.css resolves to #fff in dark and #000 in light. Neither is
 * the colour this site reads its tables in, so the colour was dragged back by
 * hand: 922 classes on td and th across 69 templates, plus 71 on elements
 * inside a cell -- the !important on that utility being the only thing beating
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

/**
 * The other direction, and the reason 76 of these classes are still in the
 * templates. Inside a cell the class is redundant -- the cell is painted
 * var(--bs-table-color) by .table > :not(caption) > * > *, this file's rule
 * sets that to var(--bs-secondary-text-emphasis), and a descendant with no
 * colour of its own inherits exactly what the utility would have forced.
 *
 * Outside a cell nothing paints it, so removing the class drops the element
 * to --bs-body-color -- a real change, not a cleanup. These assert both
 * halves, so a later sweep that strips the rest has to fail here first.
 */
for (const theme of ['dark', 'light'] as const) {
  test(`a span inside a cell needs no class of its own in ${theme}`, async ({ page }) => {
    await page.addInitScript((t) => localStorage.setItem('bsTheme', t), theme);
    await page.goto(`/${NET}/account/${ACCOUNT.address}`);

    const colour = await page.evaluate(() => {
      const cell = document.querySelector('table.table td');
      if (!cell) return null;
      const span = document.createElement('span');
      span.textContent = 'x';
      cell.appendChild(span);
      const c = getComputedStyle(span).color;
      span.remove();
      return c;
    });

    expect(colour).toBe(SOFT[theme]);
  });

  test(`the same span outside a cell would not, in ${theme}`, async ({ page }) => {
    await page.addInitScript((t) => localStorage.setItem('bsTheme', t), theme);
    await page.goto(`/${NET}/account/${ACCOUNT.address}`);

    const { bare, classed } = await page.evaluate(() => {
      const span = document.createElement('span');
      span.textContent = 'x';
      document.body.appendChild(span);
      const bare = getComputedStyle(span).color;
      span.className = 'text-secondary-emphasis';
      const classed = getComputedStyle(span).color;
      span.remove();
      return { bare, classed };
    });

    // If these were equal the remaining 76 would be removable too, and this
    // test is what says they are not.
    expect(classed).toBe(SOFT[theme]);
    expect(bare).not.toBe(classed);
  });
}
