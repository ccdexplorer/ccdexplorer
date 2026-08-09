import { test } from '@playwright/test';
import { expectPageRendered } from '../fixtures/assertions';

// tokens.py: "PLT Locks" nav item only appears for net === 'devnet'
// (see templates/base/navbar.html), unlike everything else here which is
// tested against mainnet.
test('devnet PLT locks page renders', async ({ page }) => {
  const response = await page.goto('/devnet/blockchain/plt-locks');
  await expectPageRendered(page, response);
});
