import { test, expect } from '@playwright/test';

// static/js/theme_switcher.js: #darkModeSwitch toggles html[data-bs-theme],
// persists to localStorage['bsTheme'], and swaps the navbar logo image.
test('dark mode toggle updates the theme attribute, persists, and swaps the logo', async ({
  page,
}) => {
  await page.goto('/mainnet');

  const html = page.locator('html');
  const initialTheme = await html.getAttribute('data-bs-theme');
  expect(initialTheme).toBeTruthy();

  // The checkbox itself is visually-hidden (accessible-hidden-input pattern);
  // its <label for="darkModeSwitch"> is the actual clickable surface.
  await page.locator('label[for="darkModeSwitch"]').click();
  const toggledTheme = initialTheme === 'dark' ? 'light' : 'dark';
  await expect(html).toHaveAttribute('data-bs-theme', toggledTheme);
  expect(await page.evaluate(() => localStorage.getItem('bsTheme'))).toBe(toggledTheme);

  const expectedLogo = toggledTheme === 'dark' ? 'logo_dark.png' : 'logo_light.png';
  await expect(page.locator('#logo')).toHaveAttribute('src', new RegExp(expectedLogo));

  await page.reload();
  await expect(html).toHaveAttribute('data-bs-theme', toggledTheme);
  await expect(page.locator('#darkModeSwitch')).toBeChecked({ checked: toggledTheme === 'dark' });
});
