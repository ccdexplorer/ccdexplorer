import { test } from '@playwright/test';
import fs from 'fs';
import path from 'path';
import { ACCOUNT, BLOCK, CONTRACT, MODULE, NET, NODE, TX, WALLET } from '../fixtures/ids';

/**
 * Not a test -- a measuring instrument, run before and after a change and
 * diffed. It records the computed colour of every element on a set of pages,
 * keyed by a structural path that does not depend on class attributes, so a
 * change that only removes a class token can be proven to shift nothing.
 *
 * This is how the 922 cell classes were cleared on this branch. Driven by
 * COLOUR_CAPTURE_OUT, so the caller names the baseline and the comparison.
 */

const OUT = process.env.COLOUR_CAPTURE_OUT;

// An instrument, not a test: it asserts nothing and writes files. Skipped
// unless a caller names an output directory, so `just e2e` stays a test run.
test.skip(!OUT, 'set COLOUR_CAPTURE_OUT to a directory to capture colours');

const PAGES: Record<string, string> = {
  home: `/${NET}`,
  account: `/${NET}/account/${ACCOUNT.address}`,
  block: `/${NET}/block/${BLOCK.height}`,
  transaction: `/${NET}/transaction/${TX.hash}`,
  instance: `/${NET}/instance/${CONTRACT.index}/${CONTRACT.subindex}`,
  module: `/${NET}/module/${MODULE.ref}`,
  node: `/${NET}/node/${NODE.id}`,
  staking: `/${NET}/staking`,
  smartwallet: `/${NET}/smart-wallet/${WALLET.index}/${WALLET.subindex}/${WALLET.publicKey}`,
  exchangerates: `/${NET}/exchange-rates`,
  todayin: `/${NET}/today-in`,
};


// Transactions chosen to render the logged-event templates, which are 39 of
// the 71 cell occurrences and do not appear on any fixture page: one per CIS-2
// event tag (0xff..0xfb) plus a spread of PLT contract tags. Found by querying
// tokens_logged_events_v2 for the leading byte of logged_event.
const EVENT_TXS: Record<string, string> = {
  cis2transfer: 'ffa457492691c9e964dfe6ad2027c55548703c1c2553ea801da78002b26bd9ec',
  cis2mint: '444794f46b55591d842c84b8d863184c7e506aecc146b3d9074a47b946c6995e',
  cis2burn: 'f54de23d6f10ce8ee8e31e1c27552c29d112372680e0b5061893fcdba317d2d2',
  cis2operator: 'b3546716b339916aa98f71092d64e6bab430e3bfb66d71b22ebaaa94ebfac746',
  cis2metadata: '8133650a9f200542226771a5f49699a7dabf29a0d599a862f1244ddef8c3ae31',
  plttag00: '497ccd4b877d720b075b4f9a49c787b0dbe3b74d5725c21d99253b97d77b8211',
  plttag01: 'f140e0c10474b0d0521b1b3491e1ca768d3aeab37e50903fa2066d48d0799f12',
  plttag02: '99dc68f25db30891a72e0d193de152fbdea0e457f59ba76954e3ffc5a2839df7',
  plttag03: 'dd5af5f59a737370a31a71dccb151fde705799d06d930bc4c0f3a0202b99ef7d',
  plttag04: 'f2dc9004611ed4323c555f48e378f033559ce4aa6165bac782024fd67737fe4b',
  plttage7: '4afcf26c41fc8913370868e8f00d937b7b9c9209b349bb0ca88025d853d7f232',
  plttagf1: '90a063707da37b654f0826cfb602ff9d38654baa625a88b6005f59af2403a7a0',
};
for (const [name, hash] of Object.entries(EVENT_TXS)) {
  PAGES[`tx_${name}`] = `/${NET}/transaction/${hash}`;
}

for (const theme of ['dark', 'light'] as const) {
  for (const [name, url] of Object.entries(PAGES)) {
    test(`capture ${name} ${theme}`, async ({ page }) => {
      await page.addInitScript((t) => localStorage.setItem('bsTheme', t), theme);
      await page.goto(url, { waitUntil: 'networkidle' });

      const rows = await page.evaluate(() => {
        // Structural path: child index chain from body. Stable across a
        // change that only edits class attributes.
        const pathOf = (el: Element) => {
          const parts: string[] = [];
          let node: Element | null = el;
          while (node && node.tagName !== 'BODY') {
            const parent: Element | null = node.parentElement;
            if (!parent) break;
            parts.unshift(`${node.tagName}:${Array.prototype.indexOf.call(parent.children, node)}`);
            node = parent;
          }
          return parts.join('/');
        };
        return Array.from(document.body.querySelectorAll('*')).map((el) => ({
          path: pathOf(el),
          colour: getComputedStyle(el).color,
          // Recorded for the report, never used as a key.
          hasClass: el.classList.contains('text-secondary-emphasis'),
          inCell: !!el.closest('td,th'),
        }));
      });

      // path.resolve, not join: OUT is an absolute directory.
      const dir = path.resolve(OUT as string);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `${name}.${theme}.json`), JSON.stringify(rows, null, 0));
    });
  }
}
