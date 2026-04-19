import asyncio
import concurrent.futures
from playwright.async_api import async_playwright
from db import get_connection

BROWSERS = 5
TABS = 6  # 5×6 = 30 paralel
REVIEW_API_PATTERN = "review-read/product-reviews/detailed"


def normalize_seller_filter(reviews: list, seller_id: str) -> list:
    return [
        r for r in reviews
        if str(r.get("seller", {}).get("id", "")) == str(seller_id)
    ]


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

            for media in review.get("mediaFiles", []):
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


async def tab_worker(page, products: list, browser_id: int, tab_id: int, seller_id: str) -> dict:
    total_saved = 0
    total_skipped = 0

    for product in products:
        content_id = product["contentId"]
        trendyol_product_id = product["id"]
        url = f"https://www.trendyol.com/ty/ty-p-{content_id}/yorumlar"

        print(f"[B{browser_id}T{tab_id}] 🌐 {url}")

        result = {}

        async def on_response(response):
            if (
                REVIEW_API_PATTERN in response.url
                and f"contentId={content_id}" in response.url
                and "page=0" in response.url
                and "orderBy" not in response.url
            ):
                print(f"[B{browser_id}T{tab_id}] 📡 API: {response.url[:100]}")
                try:
                    result["data"] = await response.json()
                except:
                    pass

        page.on("response", on_response)

        try:
            await page.goto(url, wait_until="networkidle", timeout=25000)
            await page.wait_for_timeout(1000)
        except Exception as e:
            print(f"[B{browser_id}T{tab_id}] ❌ Timeout: {content_id} — {str(e)[:80]}")

        page.remove_listener("response", on_response)

        if "data" in result:
            r = result["data"].get("result", {})
            summary = r.get("summary", {})
            reviews = r.get("reviews", [])
            total_count = summary.get("totalCommentCount", 0)
            avg = summary.get("averageRating", 0)
            reviews = normalize_seller_filter(reviews, seller_id)
            filtered_out = total_count - len(reviews)

            set_last_synced(trendyol_product_id, summary)

            if reviews:
                stats = save_reviews(trendyol_product_id, reviews)
                total_saved += stats["saved"]
                total_skipped += stats["skipped"]
                msg = f"[B{browser_id}T{tab_id}] ✅ {content_id}: {stats['saved']} yorum | ort: {avg}"
                if filtered_out > 0:
                    msg += f" | {filtered_out} başka satıcı filtrelendi"
                print(msg)
            else:
                if total_count > 0:
                    print(f"[B{browser_id}T{tab_id}] ⚠️ {content_id}: {total_count} yorum var ama hepsi başka satıcıya ait")
                else:
                    print(f"[B{browser_id}T{tab_id}] ℹ️ {content_id}: yorum yok")
        else:
            set_last_synced(trendyol_product_id)
            print(f"[B{browser_id}T{tab_id}] ⚠️ {content_id}: API yanıtı gelmedi")

    return {"total_saved": total_saved, "total_skipped": total_skipped}


async def browser_worker(playwright, products: list, browser_id: int, seller_id: str) -> dict:
    print(f"[B{browser_id}] 🚀 Browser başlatılıyor — {len(products)} ürün, {TABS} tab")
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        locale="tr-TR",
    )

    chunks = [products[i::TABS] for i in range(TABS)]
    pages = [await context.new_page() for _ in range(TABS)]

    print(f"[B{browser_id}] 📑 {TABS} tab açıldı")
    for i, chunk in enumerate(chunks):
        print(f"[B{browser_id}T{i+1}] {len(chunk)} ürün işlenecek")

    tasks = [
        tab_worker(pages[i], chunks[i], browser_id, i + 1, seller_id)
        for i in range(TABS)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    try:
        await browser.close()
        print(f"[B{browser_id}] ✅ Browser kapatıldı")
    except Exception as e:
        print(f"[B{browser_id}] ⚠️ Browser kapatma hatası: {e}")

    total_saved = 0
    total_skipped = 0
    for r in results:
        if isinstance(r, dict):
            total_saved += r.get("total_saved", 0)
            total_skipped += r.get("total_skipped", 0)
        elif isinstance(r, Exception):
            print(f"[B{browser_id}] ❌ Tab hatası: {r}")

    print(f"[B{browser_id}] 📊 Toplam: {total_saved} yorum kaydedildi")
    return {"total_saved": total_saved, "total_skipped": total_skipped}


async def _run_async(config_id: str, seller_id: str, content_ids: list) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, "contentId", "lastSyncedAt"
        FROM "TrendyolProduct"
        WHERE "configId" = %s
    """, (config_id,))
    all_products = {row["contentId"]: dict(row) for row in cur.fetchall()}
    cur.close()
    conn.close()

    remaining = []
    skipped_count = 0
    for product in content_ids:
        content_id = product if isinstance(product, str) else product.get("contentId")
        row = all_products.get(content_id)
        if not row or row["lastSyncedAt"] is not None:
            skipped_count += 1
            continue
        remaining.append(row)

    print(f"  ⏭️ {skipped_count} zaten çekilmiş, atlanıyor")
    print(f"  🔍 {len(remaining)} ürün çekilecek — {BROWSERS} browser × {TABS} tab = {BROWSERS*TABS} paralel")

    if not remaining:
        return {"total_saved": 0, "total_skipped": skipped_count}

    chunks = [remaining[i::BROWSERS] for i in range(BROWSERS)]

    total_saved = 0
    total_skipped = 0

    async with async_playwright() as p:
        tasks = [
            browser_worker(p, chunks[b], b + 1, seller_id)
            for b in range(BROWSERS)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, dict):
            total_saved += r.get("total_saved", 0)
            total_skipped += r.get("total_skipped", 0)

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