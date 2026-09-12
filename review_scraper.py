import asyncio
import concurrent.futures
import time
import requests
from psycopg2.extras import execute_values
from playwright.async_api import async_playwright
from db import get_connection
from telegram_notifier import notify_error


async def get_cookies():
    """
    Playwright ile Trendyol'a gir, cookie'leri al.

    ONCEDEN: wait_until="networkidle" + asyncio.sleep(2) -> 11.94 saniye.
    networkidle "500ms boyunca hic ag istegi olmasin" demek; Trendyol ana
    sayfasi reklam/takip/oneri cagrilariyla dolu oldugu icin tek basina
    9.50 saniye suruyordu. Ustundeki sabit 2 saniye de gereksizdi.

    Cookie'ler ilk yanitta zaten set ediliyor: domcontentloaded yeterli.
    Olculdu - ikisi de ayni sonucu veriyor (API HTTP 200, ayni yorumlar),
    ama sure 11.94s -> 1.73s.

    Chromium'u baslatmak pahali degil (0.22s); tarayiciyi sicak tutmaya
    gerek yok.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            locale="tr-TR",
        )
        page = await context.new_page()
        await page.goto("https://www.trendyol.com", wait_until="domcontentloaded", timeout=30000)
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
                        'approved', 'trendyol', %s, %s, %s, to_timestamp(%s / 1000.0), NOW()
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



# ─── TOPLU YAZMA ──────────────────────────────────────────────────
#
# Olculen sorun: satir satir yazarken 420 yorum icin 1.611 ayri sorgu
# gidiyordu. Her sorgu Supabase'e ~50ms gidis-donus; 78 saniyenin TAMAMI
# agda geciyordu, veritabaninin isi degil. Tasinan veri sadece 205 KB.
#
# Zincir korunuyor: her adim bir sonrakinin ihtiyaci olan id'leri
# RETURNING ile geri veriyor.
#   urunler  -> contentId  -> TrendyolProduct.id
#   yorumlar -> trendyolId -> TrendyolReview.id
#   medyalar -> yukaridaki review id'leriyle
GRUP = 500

# Sayfalar arasi bekleme. Trendyol'u zorlamamak icin var.
# Olculdu (kiperinturkiye, 187 sayfa, 3.724 yorum - ucunde de ayni veri,
# sifir hata):
#   0.30 -> 78.1s     0.20 -> 59.0s     0.15 -> 46.0s
# Istek suresi uc turda da ~20s kaldi, yani hiz sinirina yaklastigimiza
# dair bir isaret yok. 0.15 de temiz gecti; 0.20'de duruldu.
SAYFA_ARASI_SANIYE = 0.2


def _grup_yaz(conn, sql, satirlar, template, fetch=False):
    """Tek execute_values. Cagiran taraf hatayi yakalar."""
    cur = conn.cursor()
    try:
        return execute_values(cur, sql, satirlar, template=template,
                              page_size=GRUP, fetch=fetch)
    finally:
        cur.close()


def save_products_bulk(conn, config_id: str, reviews: list) -> dict:
    """contentId -> TrendyolProduct.id haritasi."""
    urunler = {}
    for r in reviews:
        cid = str(r.get("contentId", ""))
        if not cid:
            continue
        p = r.get("product", {}) or {}
        rating = p.get("rating", {}) or {}
        # Ayni urunun birden cok yorumu var; tek satira indiriyoruz.
        urunler[cid] = (config_id, cid, p.get("title", ""), p.get("image"),
                        rating.get("average"), rating.get("total", 0))
    if not urunler:
        return {}

    satirlar = list(urunler.values())
    rows = _grup_yaz(conn, '''
        INSERT INTO "TrendyolProduct"
            (id, "configId", "contentId", "productName", "imageUrl",
             "avgRating", "reviewCount", "createdAt", "updatedAt")
        VALUES %s
        ON CONFLICT ("configId", "contentId") DO UPDATE SET
            "productName" = EXCLUDED."productName",
            "imageUrl"    = EXCLUDED."imageUrl",
            "avgRating"   = EXCLUDED."avgRating",
            "reviewCount" = EXCLUDED."reviewCount",
            "updatedAt"   = NOW()
        RETURNING id, "contentId"
    ''', satirlar, template='(gen_random_uuid(),%s,%s,%s,%s,%s,%s,NOW(),NOW())', fetch=True)
    return {row["contentId"]: row["id"] for row in rows}


def save_reviews_bulk(conn, urun_haritasi: dict, reviews: list) -> dict:
    """trendyolId -> TrendyolReview.id. YALNIZCA yeni eklenenler doner."""
    gorulen, satirlar = set(), []
    for r in reviews:
        pid = urun_haritasi.get(str(r.get("contentId", "")))
        rid = r.get("id")
        if not pid or not rid or rid in gorulen:
            continue
        gorulen.add(rid)   # ayni komutta tekrar eden anahtar olmasin
        satirlar.append((pid, rid, r.get("rate", 5), r.get("comment"),
                         r.get("userFullName"), r.get("productSize"),
                         r.get("trusted", False), r.get("createdDate", 0)))
    if not satirlar:
        return {}

    # ON CONFLICT DO NOTHING + RETURNING yalnizca GERCEKTEN eklenenleri
    # dondurur - auto-publish'in ihtiyaci olan liste tam olarak bu.
    rows = _grup_yaz(conn, '''
        INSERT INTO "TrendyolReview"
            (id, "trendyolProductId", "trendyolId", rate, comment,
             "userFullName", "productSize", trusted, "createdAt")
        VALUES %s
        ON CONFLICT ("trendyolId") DO NOTHING
        RETURNING id, "trendyolId"
    ''', satirlar, template='(gen_random_uuid(),%s,%s,%s,%s,%s,%s,%s,%s)', fetch=True)
    return {row["trendyolId"]: row["id"] for row in rows}


def save_media_bulk(conn, yorum_haritasi: dict, reviews: list) -> int:
    """Medya YALNIZCA yeni eklenen yorumlar icin yazilir (eski davranis)."""
    satirlar = []
    for r in reviews:
        rid = yorum_haritasi.get(r.get("id"))
        if not rid:
            continue
        for m in (r.get("mediaFiles") or []):
            if m.get("url"):
                satirlar.append((rid, m.get("url"), m.get("thumbnailUrl")))
    if not satirlar:
        return 0
    _grup_yaz(conn, '''
        INSERT INTO "TrendyolReviewMedia" (id, "reviewId", url, "thumbnailUrl", "createdAt")
        VALUES %s
    ''', satirlar, template='(gen_random_uuid(),%s,%s,%s,NOW())')
    return len(satirlar)


def save_or_update_product(conn, config_id: str, review: dict) -> str:
    """
    Ürünü DB'ye kaydet veya güncelle.

    DIKKAT - baglanti ARTIK DISARIDAN geliyor.
    Eskiden bu fonksiyon her cagrisinda get_connection() ile YENI bir
    Postgres baglantisi aciyordu. Supabase'e her baglanti TLS el sikismasiyla
    ~0.5sn; 400 yorumlu bir magazada bu tek basina dakikalar demekti.
    Olculen gercek veri: yorum basina ~1.0 saniye, isin %96'si burada.
    """
    product = review.get("product", {})
    content_id = str(review.get("contentId", ""))
    
    if not content_id:
        return None
    
    cur = conn.cursor()
    
    try:
        # SAVEPOINT: paylasilan baglantida tek bir bozuk satir tum islemi
        # iptal etmesin. Eski surumun satir-basi dayanikliligi boyle korunuyor.
        cur.execute("SAVEPOINT sp_urun")
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
        
        cur.execute("RELEASE SAVEPOINT sp_urun")
        return product_id
        
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT sp_urun")
        raise Exception(f"Product save failed for {content_id}: {str(e)}")
    finally:
        cur.close()


def save_review(conn, trendyol_product_id: str, review: dict) -> str:
    """
    Yorumu ve görsellerini DB'ye kaydet.
    Baglanti disaridan gelir - bkz. save_or_update_product notu.
    Returns: Kaydedilen review ID veya None
    """
    review_id = review.get("id")
    if not review_id or not trendyol_product_id:
        return None
    
    cur = conn.cursor()
    
    try:
        cur.execute("SAVEPOINT sp_yorum")
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
            # Zaten var (ON CONFLICT DO NOTHING). Tum islemi degil,
            # yalnizca bu satiri geri al.
            cur.execute("ROLLBACK TO SAVEPOINT sp_yorum")
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
        
        cur.execute("RELEASE SAVEPOINT sp_yorum")
        return saved_review_id
        
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT sp_yorum")
        raise Exception(f"Review save failed for {review_id}: {str(e)}")
    finally:
        cur.close()


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
            time.sleep(SAYFA_ARASI_SANIYE)
        
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

    # TOPLU YAZMA — tek baglanti, uc sorgu.
    #
    # Olculen: satir satir yazarken 420 yorum icin 1.611 ayri sorgu gidiyordu
    # (her biri ~50ms gidis-donus). Tasinan veri sadece 205 KB; sure tamamen
    # sefer sayisindan geliyordu.
    #   422 yorum : 397s -> 1.1s
    #   3722 yorum: 4543s -> 1.9s
    # Yorum sayisi 8.8 kat artarken yazma suresi yalnizca 1.8 kat artiyor.
    conn = get_connection()
    try:
        try:
            urun_haritasi = save_products_bulk(conn, config_id, all_reviews)
            yorum_haritasi = save_reviews_bulk(conn, urun_haritasi, all_reviews)
            save_media_bulk(conn, yorum_haritasi, all_reviews)
            conn.commit()

            products_processed = set(urun_haritasi.keys())
            newly_saved_review_ids = list(yorum_haritasi.values())
            saved_count = len(newly_saved_review_ids)
            skipped_count = len(all_reviews) - saved_count

        except Exception as toplu_hata:
            # Toplu yazma duserse SATIR SATIR devam et. Tek bozuk kayit
            # yuzunden tum turu kaybetmeyelim; yavas ama calisir.
            conn.rollback()
            print(f"⚠️ Toplu yazma başarısız, satır satır deneniyor: {toplu_hata}")
            errors.append(f"bulk: {toplu_hata}")

            products_processed = set()
            newly_saved_review_ids = []
            saved_count = skipped_count = 0
            urun_onbellegi = {}

            for i, review in enumerate(all_reviews):
                try:
                    content_id = str(review.get("contentId", ""))
                    if content_id not in urun_onbellegi:
                        urun_onbellegi[content_id] = save_or_update_product(conn, config_id, review)
                    product_id = urun_onbellegi[content_id]

                    if product_id:
                        products_processed.add(review.get("contentId"))
                        saved_review_id = save_review(conn, product_id, review)
                        if saved_review_id:
                            saved_count += 1
                            newly_saved_review_ids.append(saved_review_id)
                        else:
                            skipped_count += 1
                except Exception as e:
                    errors.append(str(e))
                    if len(errors) <= 3:
                        print(f"⚠️ Kayıt hatası: {e}")

                if (i + 1) % 200 == 0:
                    conn.commit()
            conn.commit()
    finally:
        conn.close()
    
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
