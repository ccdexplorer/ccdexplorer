// The initial theme is applied synchronously by the inline script in
// base/base.html's <head>, before any CSS loads, to avoid a flash of the
// wrong theme. This file only wires up the toggle switch once the DOM is
// ready.
document.addEventListener('DOMContentLoaded', () => {
    const htmlElement = document.documentElement;
    const switchElement = document.getElementById('darkModeSwitch');
    const themeIcon = document.getElementById('darkModeSwitchIcon');
    const themeLabel = document.getElementById('darkModeSwitchLabel');
    const siteLogo = document.getElementById('logo');

    if (!switchElement) {
        return;
    }

    const siteLogoDark = '/static/logos/logo_dark.png';
    const siteLogoLight = '/static/logos/logo_light.png';

    const applyThemeToUI = (theme) => {
        switchElement.checked = theme === 'dark';
        if (themeIcon) {
            themeIcon.className = theme === 'dark' ? 'bi bi-moon-stars-fill' : 'bi bi-sun-fill';
        }
        if (themeLabel) {
            themeLabel.textContent = theme === 'dark' ? 'Dark' : 'Light';
        }
        if (siteLogo) {
            siteLogo.src = theme === 'dark' ? siteLogoDark : siteLogoLight;
        }
    };

    // Sync the switch/icon/logo with the theme the head script already applied.
    applyThemeToUI(htmlElement.getAttribute('data-bs-theme'));

    switchElement.addEventListener('change', () => {
        const theme = switchElement.checked ? 'dark' : 'light';
        localStorage.setItem('bsTheme', theme);
        // Kept in step with localStorage: the server reads the cookie to
        // know which theme to draw a chart image in.
        document.cookie = `bsTheme=${theme}; path=/; max-age=31536000; samesite=lax`;
        htmlElement.setAttribute('data-bs-theme', theme);
        applyThemeToUI(theme);
        // Picked up by htmx (hx-trigger="switched-theme from:body") to reload plots.
        document.body.dispatchEvent(new CustomEvent('switched-theme'));

        // htmx only reaches the plots it posts for. A chart drawn as an
        // <img> -- which is every tile on a category page -- is not one of
        // them: the cookie above now says light, but the browser already
        // has the dark picture for that url and will not ask again. Naming
        // the theme makes it a different url, and both are kept warm
        // server-side, so the swap costs a cache hit rather than a render.
        document.querySelectorAll('img[data-plot-src]').forEach((img) => {
            img.src = `${img.dataset.plotSrc}?theme=${theme}`;
        });

        // The index covers are stored files, one per theme, so there is no
        // parameter to add -- the theme is part of the filename.
        document.querySelectorAll('img[data-theme-src]').forEach((img) => {
            img.src = img.dataset.themeSrc.replace('{theme}', theme);
        });
    });
});
