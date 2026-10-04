import { expect, test } from '@playwright/test';
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

// The share button hands out a url. The page rewrites its own address as the
// reader changes the grouping or drags the slider, so a button holding the
// url the page was loaded with sends people a different chart from the one
// on screen -- which is the whole thing the button exists to avoid.
test(`the share url follows the chart`, async ({ page }) => {
  await page.goto(`/${NET}/charts/transaction-fees`);

  const button = page.locator('[data-share-url]');
  await expect(button).toHaveCount(1);
  await expect(button).toHaveAttribute('data-share-url', /\/weekly\//);

  await page.locator('label[for="monthly"]').click();
  await expect(page).toHaveURL(/\/monthly\//);

  const shared = await button.getAttribute('data-share-url');
  expect(shared).toBe(page.url());
});

// Chrome offers to translate the page, and a reader who accepts gets every
// text node rewritten -- including the hidden spans the slider writes its
// dates into and hx-vals reads straight back out. Production was sent
// "Juin 2021" and "Septembre 2026" from a French Chrome and answered with an
// unhandled ParserError (CCDEXPLORER-IO-2Q9, -2QB).
//
// Attributes are not translated, so the machine-readable value travels in one
// and the text is left to be whatever language the reader is reading in.
test(`a translated page still sends a readable date`, async ({ page }) => {
  await page.goto(`/${NET}/charts/transaction-fees`);
  await page.waitForFunction(
    () => !!document.getElementById('event-start-pretty')?.textContent?.trim(),
  );

  await page.evaluate(() => {
    // Exactly what the translator does: the text, not the attribute.
    document.getElementById('event-start-pretty')!.textContent = 'Juin 2021';
    document.getElementById('event-end-pretty')!.textContent = 'Septembre 2026';
  });

  const [request] = await Promise.all([
    page.waitForRequest(
      (r) => r.method() === 'POST' && r.url().includes('/charts/transaction-fees/data'),
    ),
    page.locator('label[for="monthly"]').click(),
  ]);

  const body = request.postDataJSON();
  expect(body.start_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  expect(body.end_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
});

// The theme toggle reloads the Plotly charts, because htmx is listening for
// switched-theme. The category pages draw their tiles as <img>, and nothing
// re-pointed those: the browser kept the picture it had already fetched, so
// a reader who switched to light sat looking at a grid of dark rectangles
// until they reloaded the page.
test(`category tiles follow the theme toggle`, async ({ page }) => {
  await page.goto(`/${NET}/charts/category/staking`);

  const tile = page.locator('img[data-plot-src]').first();
  await expect(tile).toHaveCount(1);
  const before = await tile.getAttribute('src');

  // The checkbox is visually-hidden; its label is the clickable surface.
  await page.locator('label[for="darkModeSwitch"]').click();
  const theme = await page.evaluate(() =>
    document.documentElement.getAttribute('data-bs-theme'),
  );

  await expect(tile).toHaveAttribute('src', new RegExp(`theme=${theme}`));
  expect(await tile.getAttribute('src')).not.toBe(before);
});

// The index covers are stored files, so they cannot follow the toggle the
// way an htmx plot does. There are two of each and the switcher picks --
// without that, a reader who asked for light went on looking at dark tiles.
test(`index covers follow the theme toggle`, async ({ page }) => {
  await page.goto(`/${NET}/charts`);

  const cover = page.locator('img[data-theme-src]').first();
  const before = await cover.getAttribute('src');
  expect(before).toMatch(/-(light|dark)\.png$/);

  await page.locator('label[for="darkModeSwitch"]').click();
  const theme = await page.evaluate(() =>
    document.documentElement.getAttribute('data-bs-theme'),
  );

  await expect(cover).toHaveAttribute('src', new RegExp(`-${theme}\\.png$`));
  expect(await cover.getAttribute('src')).not.toBe(before);
});

test(`the index serves the theme it was asked for on first paint`, async ({ browser }) => {
  // Not a toggle: a reader arriving with the cookie already set should get
  // the right tiles without the switcher having to correct them.
  const context = await browser.newContext();
  await context.addCookies([
    { name: 'bsTheme', value: 'dark', url: 'http://127.0.0.1:8011' },
  ]);
  const page = await context.newPage();
  await page.goto(`/${NET}/charts`);

  await expect(page.locator('img[data-theme-src]').first()).toHaveAttribute(
    'src',
    /-dark\.png$/,
  );
  await context.close();
});

// The category tiles asked for /plots/<name>/image.png with no theme in
// it. The server picks the theme from the bsTheme cookie and answers
// `Cache-Control: public` without `Vary: Cookie`, so the url is the same
// string for both themes and the browser reused whichever it had cached
// first: arriving in light after a visit in dark gave a grid of dark
// tiles, and only toggling fixed them -- because the toggle appends the
// theme and so asks for a different url.
test(`category tiles ask for the theme they are drawn in`, async ({ browser }) => {
  for (const theme of ['light', 'dark']) {
    const context = await browser.newContext();
    await context.addCookies([
      { name: 'bsTheme', value: theme, url: 'http://127.0.0.1:8011' },
    ]);
    const page = await context.newPage();
    await page.goto(`/${NET}/charts/category/accounts`);

    const tile = page.locator('img[data-plot-src]').first();
    await expect(tile).toHaveAttribute('src', new RegExp(`theme=${theme}`));
    await context.close();
  }
});

// A chart that picks its own resolution has no Group By radios, and the
// page read `input[name="group_by"]:checked`.id in two places without
// checking -- hx-vals when posting for the figure, and the address-bar
// rewriter afterwards. A chart that never draws is worse than a grouping
// button nobody needed.
test(`a chart with no grouping control still draws`, async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));

  await page.goto(`/${NET}/charts/fee-stabilization`);

  await expect(page.locator('input[name="group_by"]')).toHaveCount(0);
  await expect(page.locator('.js-plotly-plot')).toHaveCount(1, { timeout: 20000 });
  expect(errors).toEqual([]);
});

test(`a chart with no grouping control still rewrites its address`, async ({ page }) => {
  await page.goto(`/${NET}/charts/fee-stabilization`);
  await expect(page.locator('.js-plotly-plot')).toHaveCount(1, { timeout: 20000 });

  // The resolution it chose is recorded in the url, so a shared link still
  // says which picture it is.
  await expect(page).toHaveURL(/\/fee-stabilization\/(daily|weekly|monthly)\/\d{6}\/\d{6}/);
});
