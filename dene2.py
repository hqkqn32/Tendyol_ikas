import asyncio
import json
import sys
import nest_asyncio
from base64 import b64encode
import requests
import time

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

nest_asyncio.apply()

from playwright.async_api import async_playwright

# ─────────────────────────────────────────
# KULLANICI BİLGİLERİ
# ─────────────────────────────────────────
SELLER_ID  = "212112"
API_KEY    = "bxb4SJ75WJdnUB3IJuLP"
API_SECRET = "nN5FmdIeKwHL9RSNcWqd"
BROWSERS   = 5
TABS       = 5  # toplam 25 paralel
# ─────────────────────────────────────────

def get_content_ids():
    auth = b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    content_ids = set()

    r = requests.get(
        f"https://apigw.trendyol.com/integration/product/sellers/{SELLER_ID}/products"
        f"?supplierId={SELLER_ID}&size=50&page=0",
        headers=headers
    )
    data = r.json()
    total_pages = data.get("totalPages", 1)

    for p in data.get("content", []):
        if p.get("onSale") == True:
            content_ids.add(str(p["productContentId"]))

    for page in range(1, total_pages):
        r = requests.get(
            f"https://apigw.trendyol.com/integration/product/sellers/{SELLER_ID}/products"
            f"?supplierId={SELLER_ID}&size=50&page={page}",
            headers=headers
        )
        for p in r.json().get("content", []):
            if p.get("onSale") == True:
                content_ids.add(str(p["productContentId"]))
        time.sleep(0.3)

    print(f"Satışta olan unique ürün: {len(content_ids)}")
    return list(content_ids)


def filter_reviews_by_seller(reviews):
    return [
        rev for rev in reviews
        if str(rev.get("seller", {}).get("id", "")) == str(SELLER_ID)
    ]


def extract_review(rev):
    return {
        "id": rev.get("id"),
        "contentId": rev.get("contentId"),
        "rate": rev.get("rate"),
        "comment": rev.get("comment"),
        "userFullName": rev.get("userFullName"),
        "productSize": rev.get("productSize"),
        "productVariant": rev.get("productVariant"),
        "createdAt": rev.get("createdAt"),
        "lastModifiedAt": rev.get("lastModifiedAt"),
        "trusted": rev.get("trusted"),
        "isElite": rev.get("isElite"),
        "likesCount": rev.get("likesCount"),
        "seller": rev.get("seller"),
        "mediaFiles": [
            {
                "id": m.get("id"),
                "url": m.get("url"),
                "thumbnailUrl": m.get("thumbnailUrl"),
                "mediaType": m.get("mediaType"),
            }
            for m in rev.get("mediaFiles", [])
        ],
    }


async def tab_worker(page, content_ids, browser_id, tab_id, results):
    for cid in content_ids:
        product_url = f"https://www.trendyol.com/ty/ty-p-{cid}/yorumlar"
        result = {}
        all_captured = []  # debug

        async def on_response(response):
            url = response.url
            status = response.status
            if "review" in url.lower():
                all_captured.append(f"{status} → {url[:130]}")
            if (
                "review-read/product-reviews/detailed" in url
                and f"contentId={cid}" in url
                and "page=0" in url
                and "orderBy" not in url
            ):
                try:
                    result["data"] = await response.json()
                except:
                    pass

        page.on("response", on_response)
        try:
            await page.goto(product_url, wait_until="networkidle", timeout=20000)
            await page.wait_for_timeout(1000)
        except Exception as e:
            print(f"[B{browser_id}T{tab_id}] GOTO HATA: {cid} → {str(e)[:80]}")
        page.remove_listener("response", on_response)

        if "data" in result:
            r = result["data"].get("result", {})
            summary = r.get("summary", {})
            all_reviews = r.get("reviews", [])
            filtered = filter_reviews_by_seller(all_reviews)
            count = len(filtered)
            total_count = summary.get("totalCommentCount", 0)
            avg = summary.get("averageRating", 0)
            filtered_out = total_count - count

            if count > 0:
                extracted = [extract_review(rev) for rev in filtered]
                photo_count = sum(1 for rev in filtered if rev.get("mediaFiles"))

                results[cid] = {
                    "contentId": cid,
                    "summary": {
                        "averageRating": avg,
                        "totalCommentCount": count,
                        "totalRatingCount": summary.get("totalRatingCount"),
                        "totalImageReviewCount": summary.get("totalImageReviewCount"),
                        "totalPages": summary.get("totalPages"),
                    },
                    "reviews": extracted
                }

                msg = f"[B{browser_id}T{tab_id}] ✓ {product_url} → {count} yorum | ort: {avg} | foto: {photo_count}"
                if filtered_out > 0:
                    msg += f" | {filtered_out} başka satıcı filtrelendi"
                print(msg)
            else:
                if total_count > 0:
                    print(f"[B{browser_id}T{tab_id}] ⚠ {product_url} → hepsi başka satıcıya ait")
                else:
                    print(f"[B{browser_id}T{tab_id}] - {product_url} → yorum yok")
        else:
            print(f"[B{browser_id}T{tab_id}] ! {product_url} → veri gelmedi")
            if all_captured:
                for log in all_captured:
                    print(f"          DEBUG: {log}")
            else:
                print(f"          DEBUG: hiç review isteği yakalanmadı — sayfa yüklenemedi olabilir")


async def browser_worker(playwright, content_ids, browser_id, results):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        locale="tr-TR",
    )
    chunks = [content_ids[i::TABS] for i in range(TABS)]
    pages = [await context.new_page() for _ in range(TABS)]
    tasks = [tab_worker(pages[i], chunks[i], browser_id, i+1, results) for i in range(TABS)]
    await asyncio.gather(*tasks)
    await browser.close()


async def main():
    start = time.time()

    print("=" * 55)
    print(f"Seller ID  : {SELLER_ID}")
    print(f"Paralel    : {BROWSERS} browser × {TABS} tab = {BROWSERS*TABS}")
    print("=" * 55)

    print("\n[ADIM 1] Ürün listesi çekiliyor (onSale=true)...")
    content_ids = get_content_ids()

    total_workers = BROWSERS * TABS
    print(f"\n[ADIM 2] {len(content_ids)} ürün için yorumlar çekiliyor...\n")

    chunks = [content_ids[i::total_workers] for i in range(total_workers)]
    browser_chunks = []
    for b in range(BROWSERS):
        merged = []
        for t in range(TABS):
            idx = b * TABS + t
            if idx < len(chunks):
                merged.extend(chunks[idx])
        browser_chunks.append(merged)

    results = {}

    async with async_playwright() as p:
        tasks = [browser_worker(p, browser_chunks[b], b+1, results) for b in range(BROWSERS)]
        await asyncio.gather(*tasks)

    total_reviews = sum(len(v["reviews"]) for v in results.values())
    total_photos = sum(
        sum(1 for rev in v["reviews"] if rev.get("mediaFiles"))
        for v in results.values()
    )
    total_photo_files = sum(
        sum(len(rev.get("mediaFiles", [])) for rev in v["reviews"])
        for v in results.values()
    )

    with open("reviews_full.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    elapsed = int(time.time() - start)
    print(f"\n{'=' * 55}")
    print(f"=== BİTTİ ===")
    print(f"Süre             : {elapsed//60} dk {elapsed%60} sn")
    print(f"Taranan ürün     : {len(content_ids)}")
    print(f"Yorumlu ürün     : {len(results)}")
    print(f"Toplam yorum     : {total_reviews}")
    print(f"Fotoğraflı yorum : {total_photos}")
    print(f"Toplam fotoğraf  : {total_photo_files}")
    print(f"Seller ID filtre : {SELLER_ID} ✓")
    print(f"Kaydedildi       : reviews_full.json")
    print(f"{'=' * 55}")

asyncio.get_event_loop().run_until_complete(main())