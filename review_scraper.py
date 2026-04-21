import asyncio
import concurrent.futures
import httpx
from db import get_connection

# Playwright KALDIRILDI - Artık direkt JSON API kullanılıyor
CONCURRENT_REQUESTS = 50  # Aynı anda 50 paralel istek


def normalize_seller_filter(reviews: list, seller_id: str) -> list:
    """
    Sadece belirtilen seller_id'ye ait yorumları filtreler
    """
    return [
        r for r in reviews
        if str(r.get("seller", {}).get("id", "")) == str(seller_id)
    ]


def save_reviews(trendyol_product_id: str, reviews: list) -> dict:
    """
    Yorumları ve medya dosyalarını DB'ye kaydeder
    ON CONFLICT ile duplicate kontrolü yapar
    """
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

            # Medya dosyalarını kaydet
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
    """
    TrendyolProduct tablosunda lastSyncedAt, avgRating ve reviewCount günceller
    """
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


async def fetch_reviews_direct(
    content_id: str, 
    trendyol_product_id: str, 
    seller_id: str, 
    worker_id: int
) -> dict:
    """
    Direkt JSON API'den yorumları çeker (Playwright kullanmadan)
    
    Args:
        content_id: Trendyol ürün content ID
        trendyol_product_id: DB'deki TrendyolProduct.id (UUID)
        seller_id: Seller ID (filtreleme için)
        worker_id: Log için worker numarası
    
    Returns:
        {"total_saved": int, "total_skipped": int}
    """
    api_url = (
        f"https://public.trendyol.com/discovery-web-productreviewgateway-service/api/"
        f"review-read/product-reviews/detailed"
        f"?contentId={content_id}&page=0&culture=tr-TR"
    )
    
    total_saved = 0
    total_skipped = 0

    try:
        print(f"[W{worker_id}] 🌐 {content_id} → JSON API")
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(api_url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "tr-TR,tr;q=0.9",
            })
            
            if response.status_code != 200:
                print(f"[W{worker_id}] ❌ {content_id}: HTTP {response.status_code}")
                set_last_synced(trendyol_product_id)
                return {"total_saved": 0, "total_skipped": 0}
            
            data = response.json()
        
        # Response'u parse et
        result = data.get("result", {})
        summary = result.get("summary", {})
        all_reviews = result.get("reviews", [])
        total_count = summary.get("totalCommentCount", 0)
        avg = summary.get("averageRating", 0)
        
        # Seller'a ait yorumları filtrele
        filtered_reviews = normalize_seller_filter(all_reviews, seller_id)
        filtered_out = total_count - len(filtered_reviews)
        
        # lastSyncedAt güncelle
        set_last_synced(trendyol_product_id, summary)
        
        # Yorumları DB'ye kaydet
        if filtered_reviews:
            stats = save_reviews(trendyol_product_id, filtered_reviews)
            total_saved += stats["saved"]
            total_skipped += stats["skipped"]
            
            msg = f"[W{worker_id}] ✅ {content_id}: {stats['saved']} yorum | ort: {avg}"
            if filtered_out > 0:
                msg += f" | {filtered_out} başka satıcı filtrelendi"
            print(msg)
        else:
            if total_count > 0:
                print(f"[W{worker_id}] ⚠️ {content_id}: {total_count} yorum var ama hepsi başka satıcıya ait")
            else:
                print(f"[W{worker_id}] ℹ️ {content_id}: yorum yok")
    
    except httpx.TimeoutException:
        print(f"[W{worker_id}] ⏱️ {content_id}: Timeout (15s)")
        set_last_synced(trendyol_product_id)
    except httpx.HTTPError as e:
        print(f"[W{worker_id}] ❌ {content_id}: HTTP Error — {str(e)[:80]}")
        set_last_synced(trendyol_product_id)
    except Exception as e:
        print(f"[W{worker_id}] ❌ {content_id}: Hata — {str(e)[:80]}")
        set_last_synced(trendyol_product_id)
    
    return {"total_saved": total_saved, "total_skipped": total_skipped}


async def worker_pool(products: list, seller_id: str) -> dict:
    """
    Ürünleri paralel olarak işler
    Semaphore ile CONCURRENT_REQUESTS kadar eşzamanlı istek sınırı koyar
    
    Args:
        products: İşlenecek ürün listesi (her biri {"id": UUID, "contentId": str})
        seller_id: Seller ID
    
    Returns:
        {"total_saved": int, "total_skipped": int}
    """
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def bounded_fetch(product: dict, worker_id: int):
        """Semaphore ile sınırlandırılmış fetch"""
        async with semaphore:
            return await fetch_reviews_direct(
                content_id=product["contentId"],
                trendyol_product_id=product["id"],
                seller_id=seller_id,
                worker_id=worker_id
            )
    
    # Tüm ürünler için task oluştur
    tasks = [
        bounded_fetch(product, i + 1)
        for i, product in enumerate(products)
    ]
    
    # Paralel olarak çalıştır
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Sonuçları topla
    total_saved = 0
    total_skipped = 0
    
    for r in results:
        if isinstance(r, dict):
            total_saved += r.get("total_saved", 0)
            total_skipped += r.get("total_skipped", 0)
        elif isinstance(r, Exception):
            print(f"❌ Worker hatası: {r}")
    
    return {"total_saved": total_saved, "total_skipped": total_skipped}


async def _run_async(config_id: str, seller_id: str, content_ids: list) -> dict:
    """
    Ana scraping mantığı
    
    1. DB'den TrendyolProduct tablosunu çek
    2. Zaten çekilmiş olanları filtrele (lastSyncedAt != NULL)
    3. Kalan ürünleri paralel olarak işle
    
    Args:
        config_id: TrendyolConfig UUID
        seller_id: Trendyol Seller ID
        content_ids: İşlenecek content ID listesi
    
    Returns:
        {"total_saved": int, "total_skipped": int}
    """
    # DB'den ürünleri çek
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

    # Zaten çekilmiş olanları filtrele
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
    print(f"  🔍 {len(remaining)} ürün çekilecek — {CONCURRENT_REQUESTS} paralel istek")

    if not remaining:
        return {"total_saved": 0, "total_skipped": skipped_count}

    # Worker pool ile paralel işle
    result = await worker_pool(remaining, seller_id)
    
    print(f"\n📊 TOPLAM: {result['total_saved']} yorum kaydedildi, {result['total_skipped']} duplicate atlandı")
    
    return result


def _run_sync(config_id: str, seller_id: str, content_ids: list) -> dict:
    """
    Async fonksiyonu sync wrapper ile çalıştır
    """
    return asyncio.run(_run_async(config_id, seller_id, content_ids))


async def run(config_id: str, seller_id: str, content_ids: list) -> dict:
    """
    Ana entry point - FastAPI'den çağrılır
    
    Args:
        config_id: TrendyolConfig UUID
        seller_id: Trendyol Seller ID (örn: "212112")
        content_ids: İşlenecek content ID listesi (örn: ["12345", "67890"])
    
    Returns:
        {"total_saved": int, "total_skipped": int}
    
    Örnek:
        result = await scraper.run(
            config_id="abc-123",
            seller_id="212112",
            content_ids=["12345", "67890"]
        )
    """
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool,
            lambda: _run_sync(config_id, seller_id, content_ids)
        )
    return result