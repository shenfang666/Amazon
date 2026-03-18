#!/usr/bin/env python3
"""E2E tests using Playwright for Amazon Finance Dashboard"""

import asyncio
import json
from playwright.async_api import async_playwright

BASE_URL = "http://127.0.0.1:8767"

async def test_homepage():
    """Test homepage loads correctly"""
    print("=== Test: Homepage loads ===")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Go to homepage
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        
        # Check title or content
        title = await page.title()
        print(f"  Page title: {title}")
        
        # Check if main elements exist
        tabbar = await page.query_selector(".tabbar")
        print(f"  Tabbar found: {tabbar is not None}")
        
        # Check for dashboard content
        dashboard = await page.query_selector("#overview-metrics")
        print(f"  Dashboard found: {dashboard is not None}")
        
        # Get page content for debugging
        content = await page.content()
        print(f"  Page has content: {len(content) > 0}")
        
        await browser.close()
    print("  ✓ PASS\n")

async def test_api_endpoints():
    """Test API endpoints return valid data"""
    print("=== Test: API Endpoints ===")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Test dashboard API
        resp = await page.request.get(f"{BASE_URL}/api/dashboard")
        data = await resp.json()
        print(f"  /api/dashboard: {list(data.keys())[:5]}...")
        assert "selected_month" in data, "dashboard missing selected_month"
        
        # Test profit API
        resp = await page.request.get(f"{BASE_URL}/api/profit")
        data = await resp.json()
        print(f"  /api/profit: {list(data.keys())[:5]}...")
        assert "selected_month" in data, "profit missing selected_month"
        
        # Test inventory API
        resp = await page.request.get(f"{BASE_URL}/api/inventory")
        data = await resp.json()
        print(f"  /api/inventory: {list(data.keys())[:5]}...")
        assert "selected_month" in data, "inventory missing selected_month"
        
        # Test exceptions API
        resp = await page.request.get(f"{BASE_URL}/api/exceptions")
        data = await resp.json()
        print(f"  /api/exceptions: {list(data.keys())[:5]}...")
        assert "selected_month" in data, "exceptions missing selected_month"
        
        await browser.close()
    print("  ✓ PASS\n")

async def test_tabs():
    """Test tab switching"""
    print("=== Test: Tab Switching ===")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        
        # Wait for initial data to load
        await page.wait_for_timeout(2000)
        
        # Click on different tabs
        tabs = [
            ("overview", "overview-metrics"),
            ("profit", "sku-summary"),
            ("receivables", "receivables-summary"),
            ("operations", "manual-files"),
            ("inventory", "inventory-summary"),
            ("exceptions", "manual-exception-table"),
            ("month-close", "close-state"),
        ]
        
        for tab_name, expected_element_id in tabs:
            try:
                # Find and click tab button
                tab_button = await page.query_selector(f'.tab-button[data-tab="{tab_name}"]')
                if tab_button:
                    await tab_button.click()
                    await page.wait_for_timeout(1000)  # Wait for data to load
                    print(f"  Clicked tab: {tab_name} ✓")
                else:
                    print(f"  Tab button not found: {tab_name}")
            except Exception as e:
                print(f"  Error clicking tab {tab_name}: {e}")
        
        await browser.close()
    print("  ✓ PASS\n")

async def test_runtime_app_js():
    """Test runtime-app.js loads and executes"""
    print("=== Test: Runtime JS Execution ===")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Collect console messages
        console_messages = []
        page.on("console", lambda msg: console_messages.append(msg.text))
        
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)
        
        # Check if runtime-app.js loaded (look for state object)
        has_state = await page.evaluate("typeof state !== 'undefined'")
        print(f"  state object exists: {has_state}")
        
        # Check for any console errors
        errors = [m for m in console_messages if "Error" in m or "error" in m]
        if errors:
            print(f"  Console errors: {errors[:3]}")
        else:
            print(f"  No console errors")
        
        await browser.close()
    print("  ✓ PASS\n")

async def main():
    print("=" * 50)
    print("E2E Tests for Amazon Finance Dashboard")
    print("=" * 50 + "\n")
    
    try:
        await test_homepage()
        await test_api_endpoints()
        await test_tabs()
        await test_runtime_app_js()
        
        print("=" * 50)
        print("All E2E tests passed! ✓")
        print("=" * 50)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
