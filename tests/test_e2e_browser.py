#!/usr/bin/env python3
"""
E2E browser tests for Amazon Finance Dashboard.

Requires Playwright: pip install playwright && playwright install chromium

Usage:
    # Start server first
    PORT=8767 python server.py &
    # Then run tests (port defaults to 8767 if not set)
    python tests/test_e2e_browser.py

    # Or override port via environment
    PORT=9000 python tests/test_e2e_browser.py
"""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

BASE_URL = f"http://127.0.0.1:{os.environ.get('PORT', '8767')}"


@pytest.fixture(scope="module")
def server():
    """Start the finance server as a subprocess fixture, tear down on exit."""
    port = os.environ.get("PORT", "8767")
    project_root = Path(__file__).parent.parent
    env = os.environ.copy()
    env["PORT"] = port

    proc = subprocess.Popen(
        [sys.executable, "server.py"],
        cwd=str(project_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for server to be ready
    for _ in range(20):
        try:
            import urllib.request

            urllib.request.urlopen(BASE_URL, timeout=1)
            break
        except Exception:
            time.sleep(0.5)
    else:
        proc.terminate()
        proc.wait(timeout=5)
        pytest.fail(f"Server failed to start on {BASE_URL}")

    yield proc

    proc.terminate()
    proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_homepage_loads(server):
    """Homepage must load with tabbar and dashboard content."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)

        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")

        # Strong assertions - fail fast if elements are missing
        assert await page.query_selector(".tabbar"), "Tabbar must be present"
        assert await page.query_selector("#overview-metrics"), "Dashboard metrics grid must be present"
        assert await page.query_selector("#month-select"), "Month selector must be present"
        assert len(await page.content()) > 1000, "Page content must be non-trivial"

        # No console errors
        assert not any(errors), f"Console errors detected: {errors}"

        await browser.close()


@pytest.mark.asyncio
async def test_api_endpoints_return_valid_data(server):
    """All core API endpoints must return selected_month."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        apis = [
            "/api/dashboard",
            "/api/profit",
            "/api/inventory",
            "/api/exceptions",
            "/api/receivables",
            "/api/month-close",
        ]

        for endpoint in apis:
            resp = await page.request.get(f"{BASE_URL}{endpoint}")
            assert resp.ok, f"{endpoint} returned {resp.status}"
            data = await resp.json()
            assert "selected_month" in data, f"{endpoint} must include selected_month"
            assert isinstance(data["selected_month"], str), f"{endpoint} selected_month must be string"

        await browser.close()


@pytest.mark.asyncio
async def test_tabs_switch_and_render(server):
    """Clicking each tab must show the expected content element."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        tab_expectations = {
            "overview": "#overview-metrics",
            "profit": "#sku-summary",
            "receivables": "#receivables-summary",
            "operations": "#manual-files",
            "inventory": "#inventory-summary",
            "exceptions": "#manual-exception-table",
            "month-close": "#close-state",
        }

        for tab_name, expected_selector in tab_expectations.items():
            tab_button = page.locator(f'.tab-button[data-tab="{tab_name}"]')
            assert await tab_button.count() > 0, f'Tab button for "{tab_name}" must exist'
            await tab_button.click()
            await page.wait_for_timeout(1500)
            target = page.locator(expected_selector)
            assert await target.count() > 0, f'After clicking "{tab_name}", element "{expected_selector}" must exist'

        await browser.close()


@pytest.mark.asyncio
async def test_runtime_js_no_errors(server):
    """runtime-app.js must load and execute without console errors."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)

        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        has_state = await page.evaluate("typeof state !== 'undefined' && state !== null")
        assert has_state, "window.state must be defined after runtime-app.js executes"

        # Verify state has expected shape
        has_active_tab = await page.evaluate("typeof state.activeTab === 'string'")
        assert has_active_tab, "state.activeTab must be a string"

        assert not any(errors), f"Console errors detected after page load: {errors}"

        await browser.close()


@pytest.mark.asyncio
async def test_profit_tab_shows_profit_data(server):
    """Profit tab must display SKU summary from state.profit (not state.dashboard)."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # Switch to profit tab
        await page.locator('.tab-button[data-tab="profit"]').click()
        await page.wait_for_timeout(2000)

        # state.profit must be loaded
        has_profit = await page.evaluate("state.profit !== null && state.profit !== undefined")
        assert has_profit, "state.profit must be loaded after switching to profit tab"

        # SKU summary must be non-empty
        sku_summary_html = await page.locator("#sku-summary").inner_html()
        assert len(sku_summary_html) > 10, "SKU summary must have content"
        assert "SKU" in sku_summary_html, "SKU summary must contain SKU label"

        await browser.close()
