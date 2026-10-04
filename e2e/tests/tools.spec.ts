import { expect, test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';
import { NET } from '../fixtures/ids';

// tools.py: the "Tools" nav dropdown, mainnet-only.
for (const path of [
  `/${NET}/tools/business-accounts`,
  `/${NET}/tools/chain-information`,
  `/${NET}/tools/labeled-accounts`,
  `/${NET}/tools/validators-failed-rounds`,
  `/${NET}/tools/transactions-search`,
  `/${NET}/tools/projects`,
  `/${NET}/today-in`,
  `/${NET}/transactions-by-type`,
  `/${NET}/accounts-scheduled-release`,
  `/${NET}/accounts-cooldown`,
  '/mainnet/tools/exchange-rates',
]) {
  test(`${path} renders`, async ({ page }) => {
    const response = await page.goto(path);
    await expectPageRendered(page, response);
  });
}

// The CCD range slider showed its amount through toLocaleString(), which
// follows the browser's locale, and the page then sent that display string
// to a server that strips "," and splits on " ". A French browser renders
// 65000000 as "65 000 000 CCD": split on a space that gives "65", so the
// search silently ran for amounts over sixty-five CCD rather than over
// sixty-five million. With the narrow no-break space modern Chrome uses it
// raised ValueError instead. Seen in production alongside
// CCDEXPLORER-IO-2QB, which carried exactly that body.
test.describe('a French browser', () => {
  test.use({ locale: 'fr-FR' });

  test('sends the amount, not the way it is written', async ({ page }) => {
    const [request] = await Promise.all([
      page.waitForRequest(
        (r) => r.method() === 'POST' && r.url().includes('transactions-search/transfer'),
      ),
      page.goto(`/${NET}/tools/transactions-search`),
    ]);

    const body = request.postDataJSON();
    expect(String(body.gte)).toMatch(/^\d+$/);
    expect(String(body.lte)).toMatch(/^\d+$/);
    expect(Number(body.lte)).toBeGreaterThan(1000);
  });
});
