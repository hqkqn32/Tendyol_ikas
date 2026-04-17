import asyncio
from playwright.async_api import async_playwright

async def debug():
    content_id = "1094209514"
    url = f"https://www.trendyol.com/ty/ty-p-{content_id}/yorumlar"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        async def handle_response(response):
            if "product-reviews/detailed" in response.url and "page=0" in response.url:
                import json
                data = await response.json()
                print(json.dumps(data.get("result", {}), indent=2, ensure_ascii=False))

        page.on("response", handle_response)
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        await browser.close()

asyncio.run(debug())