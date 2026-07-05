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
        htmlElement.setAttribute('data-bs-theme', theme);
        applyThemeToUI(theme);
        // Picked up by htmx (hx-trigger="switched-theme from:body") to reload plots.
        document.body.dispatchEvent(new CustomEvent('switched-theme'));
    });
});
