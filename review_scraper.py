import asyncio
import concurrent.futures
import time
import requests
from playwright.async_api import async_playwright
from db import get_connection
from telegram_notifier import notify_error


async def get_cookies():
    """
    Playwright ile Trendyol'a gir, cookie'leri al
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            locale="tr-TR",
        )
        page = await context.new_page()
        await page.goto("https://www.trendyol.com", wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        cookies = await context.cookies()
        await browser.close()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        return cookie_str


def get_reviews_page(page: int, seller_id: str, cookie_str: str, size: int = 20):
    """
    API'den bir sayfa yorum çek
    """
    url = f"https://apigw.trendyol.com/discovery-sellerstore-gateway-service/api/ugc/product-reviews"
    url += f"?sellerId={seller_id}&page={page}&size={size}&isMarketplaceMember=true&culture=tr-TR"
    
    headers = {
        "accept": "application/json",
        "accept-language": "tr-TR,tr;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "cookie": cookie_str,
        "origin": "https://www.trendyol.com",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"API request failed: {str(e)}")


def filter_by_seller(reviews: list, seller_id: str) -> tuple:
    """
    Yorumları seller_id'ye göre filtrele
    """
    filtered = []
    filtered_out = 0
    
    for review in reviews:
        product = review.get("product", {})
        link = product.get("link", "")
        
        if f"merchantId={seller_id}" in link:
            filtered.append(review)
        else:
            filtered_out += 1
    
    return filtered, filtered_out


def auto_publish_matched_reviews(config_id: str, newly_saved_review_ids: list) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        if not newly_saved_review_ids:
            return {"matchedProducts": 0, "publishedReviews": 0, "skippedUnmatched": 0}
        
        cur.execute("""
            SELECT 
                tr.id,
                tr."trendyolId",
                tr.rate,
                tr.comment,
                tr."userFullName",
                tr."createdAt" as review_date,
                tp."productName",
                tc."storeId",
                ip."productId" as ikas_product_id,
                ip.slug as product_slug,
                tp.id as trendyol_product_id
            FROM "TrendyolReview" tr
            JOIN "TrendyolProduct" tp ON tp.id = tr."trendyolProductId"
            JOIN "TrendyolConfig" tc ON tc.id = tp."configId"
            JOIN "IkasProduct" ip ON ip.id = tp."ikasProductId"
            WHERE tr.id = ANY(%s)
            AND tp."ikasProductId" IS NOT NULL
            AND tr."importedAt" IS NULL
        """, (newly_saved_review_ids,))
        
        reviews_to_publish = cur.fetchall()
        
        if not reviews_to_publish:
            cur.execute("""
                SELECT COUNT(*) as total
                FROM "TrendyolReview" tr
                JOIN "TrendyolProduct" tp ON tp.id = tr."trendyolProductId"
                WHERE tr.id = ANY(%s)
                AND tp."ikasProductId" IS NULL
            """, (newly_saved_review_ids,))
            unmatched_count = cur.fetchone()["total"]
            return {"matchedProducts": 0, "publishedReviews": 0, "skippedUnmatched": unmatched_count}
        
        published_count = 0
        matched_product_ids = set()
        
        for review in reviews_to_publish:
            try:
                cur.execute("SAVEPOINT review_save")
                
                cur.execute("""
                    INSERT INTO "Review" (
                        id,
                        "storeId",
                        "productId",
                        "productName",
                        "productSlug",
                        "customerName",
                        rating,
                        body,
                        status,
                        source,
                        "isVerified",
                        "mediaUrls",
                        "trendyolReviewId",
                        "createdAt",
                        "updatedAt"
                    )
                    VALUES (
                        gen_random_uuid(),
                        %s, %s, %s, %s, %s, %s, %s,
                        'approved', 'trendyol', %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT DO NOTHING
                """, (
                    review["storeId"],
                    review["ikas_product_id"],
                    review["productName"],
                    review["product_slug"],
                    review["userFullName"] or 'Trendyol Müşterisi',
                    review["rate"],
                    review["comment"],
                    True,
                    [],
                    review["id"],
                    review["review_date"],
                ))
                
                # Görselleri al ve mediaUrls güncelle
                cur.execute("""
                    SELECT url FROM "TrendyolReviewMedia"
                    WHERE "reviewId" = %s
                """, (review["id"],))
                media_rows = cur.fetchall()
                media_urls = [row["url"] for row in media_rows] if media_rows else []
                
                if media_urls:
                    cur.execute("""
                        UPDATE "Review" SET "mediaUrls" = %s
                        WHERE "trendyolReviewId" = %s
                    """, (media_urls, review["id"]))
                
                cur.execute("""
                    UPDATE "TrendyolReview"
                    SET "importedAt" = NOW()
                    WHERE id = %s
                """, (review["id"],))
                
                cur.execute("RELEASE SAVEPOINT review_save")
                matched_product_ids.add(review["trendyol_product_id"])
                published_count += 1
                
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT review_save")
                print(f"⚠️ Yorum atlandı ({review['trendyolId']}): {e}")
                continue
        
        conn.commit()
        
        cur.execute("""
            SELECT COUNT(*) as total
            FROM "TrendyolReview" tr
            JOIN "TrendyolProduct" tp ON tp.id = tr."trendyolProductId"
            WHERE tr.id = ANY(%s)
            AND tp."ikasProductId" IS NULL
        """, (newly_saved_review_ids,))
        unmatched_count = cur.fetchone()["total"]
        
        return {
            "matchedProducts": len(matched_product_ids),
            "publishedReviews": published_count,
            "skippedUnmatched": unmatched_count
        }
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Auto-publish failed: {str(e)}")
    finally:
        cur.close()
        conn.close()


def save_or_update_product(config_id: str, review: dict) -> str:
    """
    Ürünü DB'ye kaydet veya güncelle
    """
    product = review.get("product", {})
    content_id = str(review.get("contentId", ""))
    
    if not content_id:
        return None
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT id FROM "TrendyolProduct"
            WHERE "configId" = %s AND "contentId" = %s
        """, (config_id, content_id))
        
        existing = cur.fetchone()
        
        product_name = product.get("title", "")
        image_url = product.get("image")
        avg_rating = product.get("rating", {}).get("average")
        review_count = product.get("rating", {}).get("total", 0)
        
        if existing:
            cur.execute("""
                UPDATE "TrendyolProduct" SET
                    "productName" = %s,
                    "imageUrl" = %s,
                    "avgRating" = %s,
                    "reviewCount" = %s,
                    "updatedAt" = NOW()
                WHERE id = %s
                RETURNING id
            """, (product_name, image_url, avg_rating, review_count, existing["id"]))
            
            result = cur.fetchone()
            product_id = result["id"] if result else existing["id"]
        else:
            cur.execute("""
                INSERT INTO "TrendyolProduct"
                    (id, "configId", "contentId", "productName", "imageUrl", 
                     "avgRating", "reviewCount", "createdAt", "updatedAt")
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id
            """, (config_id, content_id, product_name, image_url, avg_rating, review_count))
            
            result = cur.fetchone()
            product_id = result["id"]
        
        conn.commit()
        return product_id
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Product save failed for {content_id}: {str(e)}")
    finally:
        cur.close()
        conn.close()


def save_review(trendyol_product_id: str, review: dict) -> str:
    """
    Yorumu ve görsellerini DB'ye kaydet
    Returns: Kaydedilen review ID veya None
    """
    review_id = review.get("id")
    if not review_id or not trendyol_product_id:
        return None
    
    conn = get_connection()
    cur = conn.cursor()
    
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
            review.get("createdDate", 0),
        ))
        
        result = cur.fetchone()
        if not result:
            conn.rollback()
            return None
        
        saved_review_id = result["id"]
        
        media_files = review.get("mediaFiles", [])
        for media in media_files:
            if media.get("url"):
                cur.execute("""
                    INSERT INTO "TrendyolReviewMedia"
                        (id, "reviewId", url, "thumbnailUrl", "createdAt")
                    VALUES (gen_random_uuid(), %s, %s, %s, NOW())
                """, (saved_review_id, media.get("url"), media.get("thumbnailUrl")))
        
        conn.commit()
        return saved_review_id
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Review save failed for {review_id}: {str(e)}")
    finally:
        cur.close()
        conn.close()


async def _run_async(config_id: str, seller_id: str, scrape_type: str = "update") -> dict:
    """
    Ana scraping fonksiyonu
    """
    print(f"\n{'='*60}")
    print(f"🚀 Yorum çekiliyor — Seller: {seller_id} — Type: {scrape_type.upper()}")
    print(f"{'='*60}\n")
    
    t_start = time.time()
    start_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    try:
        # Cookie al
        print("🍪 Cookie alınıyor...")
        cookie_str = await get_cookies()
        print(f"✅ Cookie alındı\n")
        
    except Exception as e:
        error_msg = f"Cookie alma hatası: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
    
    # Pagination ile tüm yorumları çek
    all_reviews = []
    page = 0
    
    try:
        while True:
            data = get_reviews_page(page, seller_id, cookie_str)
            
            if not data:
                break
            
            product_reviews = data.get("productReviews", {})
            reviews = product_reviews.get("content", [])
            total_pages = product_reviews.get("totalPages", 0)
            total_elements = product_reviews.get("totalElements", 0)
            
            if not reviews:
                break
            
            filtered, filtered_out = filter_by_seller(reviews, seller_id)
            all_reviews.extend(filtered)
            
            print(f"📄 Sayfa {page + 1}/{total_pages}: {len(filtered)} ✓ | {filtered_out} ✗ | Toplam: {len(all_reviews)}/{total_elements}")
            
            if page >= total_pages - 1:
                break
            
            page += 1
            time.sleep(0.3)
        
        print(f"\n✅ Toplam {len(all_reviews)} yorum çekildi\n")
        
    except Exception as e:
        error_msg = f"API scraping hatası: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
    
    # DB'ye kaydet
    print("💾 DB'ye kaydediliyor...\n")
    
    saved_count = 0
    skipped_count = 0
    products_processed = set()
    errors = []
    newly_saved_review_ids = []
    
    for review in all_reviews:
        try:
            product_id = save_or_update_product(config_id, review)
            
            if product_id:
                products_processed.add(review.get("contentId"))
                
                saved_review_id = save_review(product_id, review)
                if saved_review_id:
                    saved_count += 1
                    newly_saved_review_ids.append(saved_review_id)
                else:
                    skipped_count += 1
        except Exception as e:
            errors.append(str(e))
            if len(errors) <= 3:
                print(f"⚠️ Kayıt hatası: {e}")
    
    # AUTO-PUBLISH: SADECE yeni eklenen ve eşleşmiş yorumları yayınla
    print("🚀 Auto-publish kontrol ediliyor...\n")
    publish_result = auto_publish_matched_reviews(config_id, newly_saved_review_ids)
    
    print(f"📢 Auto-publish sonucu:")
    print(f"   Eşleşmiş ürünler     : {publish_result['matchedProducts']}")
    print(f"   Yayınlanan yorumlar  : {publish_result['publishedReviews']}")
    print(f"   Bekleyen (eşleşmemiş): {publish_result['skippedUnmatched']}\n")
    
    t_elapsed = round(time.time() - t_start, 2)
    end_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    print(f"\n{'='*60}")
    print(f"✅ TAMAMLANDI")
    print(f"   Toplam yorum     : {len(all_reviews)}")
    print(f"   Kaydedilen       : {saved_count}")
    print(f"   Duplicate        : {skipped_count}")
    print(f"   Unique ürün      : {len(products_processed)}")
    print(f"   Süre             : {t_elapsed}s")
    if errors:
        print(f"   ⚠️ Hata sayısı   : {len(errors)}")
    print(f"{'='*60}\n")
    
    # runTimeLog
    runtime_log = {
        "scrapeType": scrape_type,
        "startTime": start_time,
        "endTime": end_time,
        "duration": t_elapsed,
        "scrapedData": {
            "totalScraped": len(all_reviews),
            "newReviews": saved_count,
            "duplicateReviews": skipped_count,
            "uniqueProducts": len(products_processed)
        },
        "autoPublished": publish_result,
        "errors": errors[:10] if errors else []
    }
    
    return {
        "total_saved": saved_count,
        "total_skipped": skipped_count,
        "unique_products": len(products_processed),
        "elapsed": t_elapsed,
        "errors": errors[:10] if errors else [],
        "runtime_log": runtime_log
    }


def _run_sync(config_id: str, seller_id: str, scrape_type: str = "update") -> dict:
    return asyncio.run(_run_async(config_id, seller_id, scrape_type))


async def run(config_id: str, seller_id: str, scrape_type: str = "update") -> dict:
    """
    Entry point - FastAPI/Queue Manager'dan çağrılır
    """
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool,
            lambda: _run_sync(config_id, seller_id, scrape_type)
        )
    return result
