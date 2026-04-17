import asyncio
import concurrent.futures
from playwright.async_api import async_playwright
from db import get_connection

REVIEW_API_PATTERN = "product-reviews/detailed"
PARALLEL_TABS = 3


async def fetch_reviews_for_product(page, content_id: str, seller_id: str = None) -> dict:
    url = f"https://www.trendyol.com/ty/ty-p-{content_id}/yorumlar"
    all_reviews = []
    summary = {}
    captured = []

    async def handle_response(response):
        if REVIEW_API_PATTERN in response.url and "page=0" in response.url:
            try:
                data = await response.json()
                captured.append(data)
            except:
                pass

    page.on("response", handle_response)

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(1500)
    except Exception as e:
        print(f"❌ Sayfa yüklenemedi {content_id}: {e}")
        page.remove_listener("response", handle_response)
        return {"reviews": [], "summary": {}}

    page.remove_listener("response", handle_response)

    if not captured:
        return {"reviews": [], "summary": {}}

    first_page = captured[0]
    result = first_page.get("result", {})
    summary = result.get("summary", {})
    total_pages = summary.get("totalPages", 0)
    reviews = result.get("reviews", [])

    if seller_id:
        reviews = [r for r in reviews if str(r.get("seller", {}).get("id", "")) == str(seller_id)]

    all_reviews.extend(reviews)
    print(f"  📄 Sayfa 1/{total_pages} — {len(reviews)} yorum")

    for pg in range(1, total_pages):
        next_url = f"https://www.trendyol.com/ty/ty-p-{content_id}/yorumlar?sayfa={pg + 1}"
        next_captured = []

        async def handle_next(response):
            if REVIEW_API_PATTERN in response.url:
                try:
                    data = await response.json()
                    next_captured.append(data)
                except:
                    pass

        page.on("response", handle_next)

        try:
            await page.goto(next_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(1500)
        except:
            page.remove_listener("response", handle_next)
            break

        page.remove_listener("response", handle_next)

        if next_captured:
            next_reviews = next_captured[0].get("result", {}).get("reviews", [])
            if seller_id:
                next_reviews = [r for r in next_reviews if str(r.get("seller", {}).get("id", "")) == str(seller_id)]
            all_reviews.extend(next_reviews)
            print(f"  📄 Sayfa {pg + 1}/{total_pages} — {len(next_reviews)} yorum")

        await asyncio.sleep(1)

    return {"reviews": all_reviews, "summary": summary}


def save_reviews(trendyol_product_id: str, reviews: list) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for review in reviews:
        review_id = review.get("id")
        if not review_id:
            continue

        try:
            cur.execute("""
                INSERT INTO "TrendyolReview"
                    (id, "trendyolProductId", "trendyolId", rate, comment, "userFullName",
                     "productSize", trusted, "createdAt")
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("trendyolId") DO NOTHING
                RETURNING id
            """, (
                trendyol_product_id,
                review_id,
                review.get("rate", 5),
                review.get("comment"),
                review.get("userFullName"),
                review.get("productSize"),
                review.get("trusted", False),
                review.get("createdAt", 0),
            ))

            row = cur.fetchone()
            if not row:
                skipped += 1
                continue

            rv_id = row["id"]
            saved += 1

            media_files = review.get("mediaFiles", [])
            for media in media_files:
                if media.get("url"):
                    cur.execute("""
                        INSERT INTO "TrendyolReviewMedia"
                            (id, "reviewId", url, "thumbnailUrl", "createdAt")
                        VALUES (gen_random_uuid(), %s, %s, %s, NOW())
                    """, (rv_id, media.get("url"), media.get("thumbnailUrl")))

        except Exception as e:
            print(f"⚠️ Review kayıt hatası {review_id}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    return {"saved": saved, "skipped": skipped}


def set_last_synced(trendyol_product_id: str, summary: dict = None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE "TrendyolProduct" SET
            "avgRating" = %s,
            "reviewCount" = %s,
            "lastSyncedAt" = NOW(),
            "updatedAt" = NOW()
        WHERE id = %s
    """, (
        summary.get("averageRating") if summary else None,
        summary.get("totalCommentCount", 0) if summary else 0,
        trendyol_product_id,
    ))
    conn.commit()
    cur.close()
    conn.close()


async def process_product(browser, semaphore, product_row: dict, seller_id: str, index: int, total: int):
    async with semaphore:
        content_id = product_row["contentId"]
        trendyol_product_id = product_row["id"]

        print(f"\n🔍 [{index}/{total}] ContentId: {content_id}")

        if product_row["lastSyncedAt"] is not None:
            print(f"  ⏭️ [{index}/{total}] Zaten çekilmiş, atlanıyor")
            return {"saved": 0, "skipped": 0}

        page = await browser.new_page()
        try:
            result = await fetch_reviews_for_product(page, content_id, seller_id=seller_id)
        finally:
            await page.close()

        reviews = result.get("reviews", [])
        summary = result.get("summary", {})

        set_last_synced(trendyol_product_id, summary)

        if not reviews:
            print(f"  ℹ️ [{index}/{total}] Yorum yok")
            return {"saved": 0, "skipped": 0}

        stats = save_reviews(trendyol_product_id, reviews)
        print(f"  ✅ [{index}/{total}] {stats['saved']} yorum kaydedildi")
        return stats


async def _run_async(config_id: str, seller_id: str, content_ids: list) -> dict:
    total_saved = 0
    total_skipped = 0
    total = len(content_ids)

    # Tek sorguda tüm ürünleri çek
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT "contentId", id, "lastSyncedAt"
        FROM "TrendyolProduct"
        WHERE "configId" = %s
    """, (config_id,))
    all_products = {row["contentId"]: dict(row) for row in cur.fetchall()}
    cur.close()
    conn.close()

    # Sadece çekilmemiş olanları filtrele
    remaining = []
    skipped_count = 0
    for product in content_ids:
        content_id = product if isinstance(product, str) else product.get("contentId")
        row = all_products.get(content_id)
        if not row:
            continue
        if row["lastSyncedAt"] is not None:
            skipped_count += 1
            continue
        remaining.append(row)

    print(f"  ⏭️ {skipped_count} ürün zaten çekilmiş, atlanıyor")
    print(f"  🔍 {len(remaining)} ürün çekilecek")

    if not remaining:
        return {"total_saved": 0, "total_skipped": skipped_count}

    semaphore = asyncio.Semaphore(PARALLEL_TABS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        tasks = []
        for i, product_row in enumerate(remaining):
            task = process_product(browser, semaphore, product_row, seller_id, i + 1, len(remaining))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        await browser.close()

    for r in results:
        if isinstance(r, dict):
            total_saved += r.get("saved", 0)
            total_skipped += r.get("skipped", 0)

    return {"total_saved": total_saved, "total_skipped": total_skipped}


def _run_sync(config_id: str, seller_id: str, content_ids: list) -> dict:
    return asyncio.run(_run_async(config_id, seller_id, content_ids))


async def run(config_id: str, seller_id: str, content_ids: list) -> dict:
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool,
            lambda: _run_sync(config_id, seller_id, content_ids)
        )
    return result