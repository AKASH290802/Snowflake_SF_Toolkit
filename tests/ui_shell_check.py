import os
from pathlib import Path
import unittest

from playwright.sync_api import sync_playwright, expect


class ApplicationShellTests(unittest.TestCase):
    def test_startup_covers_sidebar_and_viewport(self):
        output = Path(os.environ.get('TEMP', '.')) / 'sf_bulk_ui_checks'
        output.mkdir(exist_ok=True)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel='msedge', headless=True)
            try:
                for width, height in ((1440, 1000), (390, 844)):
                    with self.subTest(width=width):
                        page = browser.new_page(viewport={'width': width, 'height': height})
                        page.goto(os.environ.get('SF_UI_URL', 'http://localhost:8501'))
                        splash = page.locator('body > dialog.sf-splash[open]')
                        expect(splash).to_be_visible(timeout=60000)
                        splash.get_by_role('button', name='Close', exact=True).click()
                        page.reload()
                        expect(splash).to_be_visible(timeout=30000)
                        self.assertEqual(splash.bounding_box(), {
                            'x': 0, 'y': 0, 'width': width, 'height': height,
                        })
                        self.assertTrue(splash.evaluate("element => element.matches(':modal')"))
                        for horizontal, vertical in ((2, 2), (2, height - 2),
                                                     (width - 2, 2), (width - 2, height - 2),
                                                     (20, 200)):
                            self.assertTrue(page.evaluate(
                                'point => !!document.elementFromPoint(...point).closest("dialog.sf-splash")',
                                [horizontal, vertical],
                            ))
                        expect(page.locator('[data-testid=stException]')).to_have_count(0)
                        page.screenshot(path=str(output / f'fullscreen-startup-{width}.png'))
                        expect(splash).to_have_count(0, timeout=3500)
                        expect(page.get_by_role('tab', name='Insert', exact=False).first).to_be_visible()
                        page.close()
            finally:
                browser.close()

    def test_sidebar_can_reopen(self):
        output = Path(os.environ.get('TEMP', '.')) / 'sf_bulk_ui_checks'
        output.mkdir(exist_ok=True)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel='msedge', headless=True)
            try:
                for width, height in ((1440, 1000), (390, 844)):
                    with self.subTest(width=width):
                        page = browser.new_page(viewport={'width': width, 'height': height})
                        page.goto(os.environ.get('SF_UI_URL', 'http://localhost:8501'))
                        page.locator('dialog.sf-splash[open]').get_by_role('button', name='Close', exact=True).click(timeout=60000)
                        sidebar = page.locator('[data-testid="stSidebar"]')
                        collapse = page.locator('[data-testid="stSidebarCollapseButton"] button')
                        reopen = page.locator('[data-testid="stExpandSidebarButton"]')
                        for theme in ('Dark', 'Light'):
                            page.locator('.sf-theme-toggle:visible').click(timeout=5000)
                            panel = page.locator('.sf-theme-panel:popover-open')
                            panel.get_by_role('button', name=theme, exact=True).click()
                            page.keyboard.press('Escape')
                            collapse.click()
                            expect(sidebar).to_have_attribute('aria-expanded', 'false')
                            expect(reopen).to_be_visible()
                            reopen.hover(timeout=5000)
                            self.assertTrue(reopen.evaluate('element => { const box = element.getBoundingClientRect(); return element.contains(document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2)); }'))
                            page.screenshot(path=str(output / f'sidebar-collapsed-{theme}-{width}.png'))
                            reopen.click()
                            expect(sidebar).to_have_attribute('aria-expanded', 'true')
                            expect(collapse).to_be_visible()
                            collapse.click()
                            reopen.focus()
                            page.keyboard.press('Enter')
                            expect(sidebar).to_have_attribute('aria-expanded', 'true')
                        expect(page.locator('#MainMenu:visible, [data-testid=stMainMenu]:visible')).to_have_count(0)
                        page.close()
            finally:
                browser.close()

    def test_desktop_and_mobile_shell(self):
        output = Path(os.environ.get('TEMP', '.')) / 'sf_bulk_ui_checks'
        output.mkdir(exist_ok=True)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel='msedge', headless=True)
            try:
                for width, height in ((1440, 1000), (390, 844)):
                    with self.subTest(width=width):
                        page = browser.new_page(viewport={'width': width, 'height': height})
                        page.goto(os.environ.get('SF_UI_URL', 'http://localhost:8501'))
                        splash = page.locator('dialog.sf-splash[open]')
                        expect(splash).to_be_visible(timeout=60000)
                        self.assertEqual(splash.bounding_box(), {
                            'x': 0, 'y': 0, 'width': width, 'height': height,
                        })
                        self.assertTrue(page.evaluate(
                            "document.elementFromPoint(20, 200).closest('dialog.sf-splash') !== null"
                        ))
                        page.screenshot(path=str(output / f'splash-{width}.png'))
                        expect(splash).to_have_count(0, timeout=3500)
                        toggle = page.locator('.sf-theme-toggle:visible')
                        expect(toggle).to_have_count(1, timeout=30000)
                        expect(page.locator('#MainMenu:visible, [data-testid=stMainMenu]:visible')).to_have_count(0)
                        expect(page.locator('[data-testid=stException]')).to_have_count(0)
                        page.screenshot(path=str(output / f'app-{width}.png'))
                        toggle.click(timeout=5000)
                        panel = page.locator('.sf-theme-panel:popover-open')
                        expect(panel).to_be_visible()
                        panel.get_by_role('button', name='Dark', exact=True).click()
                        expect(page.locator('html')).to_have_attribute('data-sf-theme', 'dark')
                        dark_background = page.locator('.stApp').evaluate('element => getComputedStyle(element).backgroundImage')
                        page.screenshot(path=str(output / f'theme-dark-{width}.png'))
                        panel.get_by_role('button', name='Light', exact=True).click()
                        expect(page.locator('html')).to_have_attribute('data-sf-theme', 'light')
                        self.assertNotEqual(dark_background, page.locator('.stApp').evaluate(
                            'element => getComputedStyle(element).backgroundImage'))
                        page.evaluate("palette => localStorage.setItem('sf_bulk_palette_v1', JSON.stringify(palette))", {
                            'background': '#75c1e8', 'panels': '#fff7dd',
                            'headers': '#425b9a', 'buttons': '#ff91a4',
                        })
                        page.reload()
                        toggle.click(timeout=15000)
                        expect(panel.get_by_label('Sidebar background color', exact=True)).to_have_value('#fff7dd')
                        expect(panel.get_by_label('Background color', exact=True)).to_have_value('#75c1e8')
                        for preset in ('Sky & Rose', 'Midnight & Cream', 'Teal & Scarlet',
                                       'Olive & Clay', 'Denim & Orange', 'Forest & Gold'):
                            panel.get_by_role('button', name=preset, exact=True).click()
                            expect(panel.locator('.sf-palette-swatch[aria-pressed="true"]')).to_have_count(1)
                            colors = panel.locator('input[type="color"]').evaluate_all('inputs => inputs.map(input => input.value)')
                            self.assertEqual(len(set(colors)), 4)
                        panel.get_by_role('button', name='Sky & Rose', exact=True).click()
                        background_input = panel.get_by_label('Background color', exact=True)
                        sidebar_input = panel.get_by_label('Sidebar background color', exact=True)
                        panel_input = panel.get_by_label('Panel color', exact=True)
                        header_input = panel.get_by_label('Header color', exact=True)
                        button_input = panel.get_by_label('Button color', exact=True)
                        expect(background_input).to_have_value('#75c1e8')
                        expect(header_input).to_have_value('#425b9a')
                        expect(button_input).to_have_value('#ff91a4')
                        app = page.locator('.stApp')
                        sidebar = page.locator('[data-testid="stSidebar"]')
                        hero = page.locator('.sf-hero')
                        background = 'element => { const style = getComputedStyle(element); return style.backgroundColor + style.backgroundImage; }'
                        old_page = app.evaluate(background)
                        old_sidebar = sidebar.evaluate(background)
                        old_header = hero.evaluate(background)
                        background_input.fill('#add8e6')
                        self.assertNotEqual(app.evaluate(background), old_page)
                        self.assertEqual(sidebar.evaluate(background), old_sidebar)
                        self.assertEqual(hero.evaluate(background), old_header)
                        expect(button_input).to_have_value('#ff91a4')
                        custom_background = app.evaluate(background)
                        button_input.fill('#f0c020')
                        self.assertEqual(app.evaluate(background), custom_background)
                        self.assertEqual(hero.evaluate(background), old_header)
                        self.assertEqual(page.evaluate(
                            "getComputedStyle(document.documentElement).getPropertyValue('--sf-primary').trim()"), '#f0c020')
                        primary = page.locator('.stButton button[kind="primary"]').first
                        expect(primary).to_have_css('background-color', 'rgb(240, 192, 32)')
                        expect(primary).to_have_css('color', 'rgb(0, 0, 0)')
                        search_button = page.locator('#sf-cmdk-fab')
                        expect(search_button).to_have_css('background-image', primary.evaluate(
                            'element => getComputedStyle(element).backgroundImage'))
                        expect(search_button).to_have_css('color', 'rgb(0, 0, 0)')
                        self.assertIn('linear-gradient', primary.evaluate('element => getComputedStyle(element).backgroundImage'))
                        self.assertEqual(page.locator('.sf-hero').evaluate(
                            "element => getComputedStyle(element, '::after').backgroundImage"), 'none')
                        header_input.fill('#123456')
                        expect(hero).to_have_css('background-color', 'rgb(18, 52, 86)')
                        expect(hero.locator('.sf-hero-title')).to_have_css('color', 'rgb(255, 255, 255)')
                        old_panel = panel.evaluate(background)
                        activity_frame = page.frame_locator('iframe[srcdoc*="sfa-root"]')
                        activity = activity_frame.locator('#sfa-root')
                        panel_surface = page.locator('.sf-conn-card').first
                        computed_image = 'element => getComputedStyle(element).backgroundImage'
                        expect(activity).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        panel_input.fill('#f0eee6')
                        expect(activity).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        self.assertEqual(sidebar.evaluate(background), old_sidebar)
                        self.assertNotEqual(panel.evaluate(background), old_panel)
                        self.assertEqual(app.evaluate(background), custom_background)
                        custom_panel = panel.evaluate(background)
                        custom_header = hero.evaluate(background)
                        sidebar_input.fill('#85c7de')
                        self.assertNotEqual(sidebar.evaluate(background), old_sidebar)
                        self.assertEqual(panel.evaluate(background), custom_panel)
                        self.assertEqual(app.evaluate(background), custom_background)
                        self.assertEqual(hero.evaluate(background), custom_header)
                        expect(panel.locator('.sf-palette-swatch[aria-pressed="true"]')).to_have_count(0)
                        expect(button_input).to_have_value('#f0c020')
                        panel.get_by_role('button', name='Dark', exact=True).click()
                        expect(activity).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        expect(activity).to_have_css('color', 'rgb(240, 244, 255)')
                        self.assertNotEqual(app.evaluate(background), custom_background)
                        dark_sidebar = sidebar.evaluate(background)
                        sidebar_input.fill('#397254')
                        self.assertNotEqual(sidebar.evaluate(background), dark_sidebar)
                        sidebar_input.fill('#85c7de')
                        expect(header_input).to_have_value('#123456')
                        expect(button_input).to_have_value('#f0c020')
                        page.screenshot(path=str(output / f'theme-custom-dark-{width}.png'))
                        panel.get_by_role('button', name='Light', exact=True).click()
                        panel_box = panel.bounding_box()
                        self.assertGreaterEqual(panel_box['x'], 0)
                        self.assertLessEqual(panel_box['x'] + panel_box['width'], width)
                        self.assertLessEqual(panel_box['y'] + panel_box['height'], height)
                        page.screenshot(path=str(output / f'theme-custom-{width}.png'))
                        page.keyboard.press('Escape')
                        expect(panel).to_have_count(0)
                        activity.screenshot(path=str(output / f'activity-palette-{width}.png'))
                        recent_card = activity_frame.locator('.sfr-card').first
                        recent_card.hover()
                        expect(recent_card).to_have_css('background-image', activity.evaluate(computed_image))
                        recent_card.locator('.sfr-sum').click()
                        expect(recent_card).to_have_class('sfr-card open')
                        expect(recent_card).to_have_css('background-image', activity.evaluate(computed_image))
                        toggle.click()
                        panel.get_by_role('button', name='Dark', exact=True).click()
                        expect(recent_card).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        page.keyboard.press('Escape')
                        recent_card.screenshot(path=str(output / f'recent-run-dark-{width}.png'))
                        toggle.click()
                        panel.get_by_role('button', name='Light', exact=True).click()
                        page.keyboard.press('Escape')
                        expect(recent_card).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        search_button.hover()
                        self.assertNotIn('168, 85, 247', search_button.evaluate('element => getComputedStyle(element).boxShadow'))
                        search_button.click()
                        search_panel = page.locator('#sf-cmdk-panel')
                        expect(search_panel).to_be_visible()
                        expect(search_panel).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        expect(page.locator('#sf-cmdk-input')).to_have_css('color', 'rgb(32, 38, 50)')
                        page.locator('#sf-cmdk-input').click()
                        page.locator('#sf-cmdk-input').fill('Update')
                        expect(page.locator('#sf-cmdk-list li')).to_have_count(1)
                        expect(page.locator('#sf-cmdk-list li')).to_contain_text('Update')
                        page.screenshot(path=str(output / f'search-palette-{width}.png'))
                        page.keyboard.press('Escape')
                        expect(search_panel).not_to_be_visible()
                        toggle.click(timeout=5000)
                        expect(panel).to_be_visible()
                        toggle.click(timeout=5000)
                        expect(panel).to_have_count(0)
                        if width < 640:
                            page.locator('[data-testid="stSidebarCollapseButton"] button').click(timeout=5000)
                        page.get_by_role('tab', name='Update', exact=False).first.click()
                        expect(splash).to_have_count(0)
                        page.reload()
                        toggle.click(timeout=15000)
                        expect(panel.get_by_label('Background color', exact=True)).to_have_value('#add8e6')
                        expect(panel.get_by_label('Sidebar background color', exact=True)).to_have_value('#85c7de')
                        expect(panel.get_by_label('Panel color', exact=True)).to_have_value('#f0eee6')
                        expect(panel.get_by_label('Header color', exact=True)).to_have_value('#123456')
                        expect(panel.get_by_label('Button color', exact=True)).to_have_value('#f0c020')
                        expect(activity).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        expect(search_button).to_have_css('color', 'rgb(0, 0, 0)')
                        expect(page.locator('html')).to_have_attribute('data-sf-theme', 'light')
                        panel.get_by_role('button', name='Reset palette', exact=True).click()
                        self.assertEqual(page.evaluate("localStorage.getItem('sf_bulk_accent_v1')"), None)
                        self.assertEqual(page.evaluate("localStorage.getItem('sf_bulk_palette_v1')"), None)
                        self.assertFalse(page.evaluate("document.documentElement.hasAttribute('data-sf-custom-color')"))
                        self.assertEqual(page.evaluate("document.documentElement.style.getPropertyValue('--sf-page-wash')"), '')
                        self.assertEqual(page.evaluate("document.documentElement.style.getPropertyValue('--sf-sidebar-wash')"), '')
                        expect(activity).to_have_css('background-image', panel_surface.evaluate(computed_image))
                        expect(search_button).to_have_css('background-color', 'rgb(15, 118, 110)')
                        page.close()
            finally:
                browser.close()
        print(f'Browser screenshots: {output}')


if __name__ == '__main__':
    unittest.main()