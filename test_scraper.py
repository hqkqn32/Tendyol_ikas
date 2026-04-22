import asyncio
import json
import time
import requests
from playwright.async_api import async_playwright

SELLER_ID = "212112"  # TEST SELLER
BASE_URL = "https://apigw.trendyol.com/discovery-sellerstore-gateway-service/api/ugc/product-reviews"


async def get_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            locale="tr-TR",
        )
        page = await context.new_page()
        await page.goto("https://www.trendyol.com", wait_until="networkidle")
        await asyncio.sleep(2)
        cookies = await context.cookies()
        await browser.close()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])


def get_reviews_page(page: int, cookie_str: str, size: int = 20):
    url = f"{BASE_URL}?sellerId={SELLER_ID}&page={page}&size={size}&isMarketplaceMember=true&culture=tr-TR"
    
    headers = {
        "accept": "application/json",
        "cookie": cookie_str,
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API Hatası: {e}")
        return None


def filter_by_seller(reviews: list, seller_id: str):
    filtered = []
    for review in reviews:
        link = review.get("product", {}).get("link", "")
        if f"merchantId={seller_id}" in link:
            filtered.append(review)
    return filtered


async def main():
    print(f"{'='*60}")
    print(f"🧪 TEST MODU - Seller {SELLER_ID}")
    print(f"⚠️  DB'YE KAYDETMEZ - Sadece API test")
    print(f"{'='*60}\n")
    
    t_start = time.time()
    
    cookie_str = await get_cookies()
    print(f"✅ Cookie alındı\n")
    
    all_reviews = []
    page = 0
    
    while True:
        data = get_reviews_page(page, cookie_str)
        
        if not data:
            break
        
        product_reviews = data.get("productReviews", {})
        reviews = product_reviews.get("content", [])
        total_pages = product_reviews.get("totalPages", 0)
        
        if not reviews:
            break
        
        filtered = filter_by_seller(reviews, SELLER_ID)
        all_reviews.extend(filtered)
        
        print(f"📄 Sayfa {page + 1}/{total_pages}: {len(filtered)} yorum | Toplam: {len(all_reviews)}")
        
        if page >= total_pages - 1:
            break
        
        page += 1
        time.sleep(0.3)
    
    t_elapsed = round(time.time() - t_start, 2)
    
    # JSON'a kaydet (DB'ye DEĞİL!)
    with open("test_output.json", "w", encoding="utf-8") as f:
        json.dump(all_reviews, f, ensure_ascii=False, indent=2)
    
    # Analiz
    products = {}
    total_media = 0
    
    for review in all_reviews:
        content_id = review.get("contentId")
        if content_id:
            if content_id not in products:
                products[content_id] = {"count": 0, "media": 0}
            products[content_id]["count"] += 1
            products[content_id]["media"] += len(review.get("mediaFiles", []))
            total_media += len(review.get("mediaFiles", []))
    
    print(f"\n{'='*60}")
    print(f"✅ TEST TAMAMLANDI")
    print(f"   Toplam yorum  : {len(all_reviews)}")
    print(f"   Unique ürün   : {len(products)}")
    print(f"   Toplam görsel : {total_media}")
    print(f"   Süre          : {t_elapsed}s")
    print(f"   Dosya         : test_output.json")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())