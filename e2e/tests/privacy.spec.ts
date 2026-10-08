import { expect, test } from '@playwright/test';
import { NET } from '../fixtures/ids';

// A security report flagged two things: webfonts fetched from Google, which
// hands the reader's IP and user agent to a third party before they have
// done anything, and a cookie written on every first visit before the
// reader had chosen anything or been asked.

test('no third party is contacted when a page loads', async ({ page }) => {
  const external: string[] = [];
  page.on('request', (request) => {
    const host = new URL(request.url()).hostname;
    if (!host.startsWith('127.0.0.1') && host !== 'localhost') external.push(host);
  });

  await page.goto(`/${NET}`);
  await page.waitForLoadState('networkidle');

  expect(external).toEqual([]);
});

test('the font is served from this origin', async ({ page }) => {
  await page.goto(`/${NET}`);
  await page.waitForLoadState('networkidle');

  const loaded = await page.evaluate(() =>
    [...document.fonts].map((f) => f.family).includes('Outfit'),
  );
  expect(loaded).toBe(true);
});

test('a first visit sets no cookie at all', async ({ browser }) => {
  // Nobody has chosen anything yet, so there is no preference to remember
  // -- writing one is a default asserted, not a preference stored.
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(`/${NET}`);
  await page.waitForTimeout(1000);

  expect(await context.cookies()).toEqual([]);
  await context.close();
});

test('choosing a theme is what writes the cookie', async ({ browser }) => {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(`/${NET}`);

  await page.locator('label[for="darkModeSwitch"]').click();
  await page.waitForTimeout(500);

  const cookies = await context.cookies();
  expect(cookies.map((c) => c.name)).toEqual(['bsTheme']);
  await context.close();
});

test('a reader with no cookie still gets charts in the page theme', async ({ browser }) => {
  // The cookie is how an <img> tells the server which theme to draw in.
  // Without one the server falls back to dark, which is what the page
  // itself defaults to, so the two still agree.
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(`/${NET}/charts`);

  await expect(page.locator('html')).toHaveAttribute('data-bs-theme', 'dark');
  await expect(page.locator('img[data-theme-src]').first()).toHaveAttribute(
    'src',
    /-dark\.png$/,
  );
  await context.close();
});
