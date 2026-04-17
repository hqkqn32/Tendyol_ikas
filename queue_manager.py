import asyncio
from db import get_connection, get_pending_queue, update_queue_status
from product_fetcher import run as run_product_fetcher
from review_scraper import run as run_review_scraper
from llm_matcher import run as run_llm_matcher
from review_importer import run as run_review_importer

POLL_INTERVAL = 30  # saniye

async def process_job(job: dict):
    queue_id = job["id"]
    config_id = job["configId"]
    store_id = job["storeId"]
    seller_id = job["sellerId"]
    api_key = job["apiKey"]
    api_secret = job["apiSecret"]

    print(f"\n{'='*60}")
    print(f"🚀 İş başladı — queue_id: {queue_id}")
    print(f"   store_id: {store_id} | seller_id: {seller_id}")

    update_queue_status(queue_id, "running")

    try:
        # ADIM 1 — Ürünleri çek
        print(f"\n[1/4] Ürünler çekiliyor...")
        products = run_product_fetcher(config_id, seller_id, api_key, api_secret)

        if not products:
            raise Exception("Ürün listesi boş geldi")

        # Supabase'e yazıldığından emin ol
        print(f"✅ {len(products)} ürün kaydedildi, review scraping başlıyor...")

        # ADIM 2 — Yorumları çek
        print(f"\n[2/4] Yorumlar çekiliyor — {len(products)} ürün...")

        # DB'den contentId listesini al (kayıtlı olanları)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT "contentId" FROM "TrendyolProduct"
            WHERE "configId" = %s
        """, (config_id,))
        db_products = [{"contentId": r["contentId"]} for r in cur.fetchall()]
        cur.close()
        conn.close()

        print(f"   DB'den {len(db_products)} ürün alındı")
        scrape_stats = await run_review_scraper(config_id, seller_id, db_products)
        print(f"  → {scrape_stats['total_saved']} yorum kaydedildi")

        # ADIM 3 — LLM eşleştirme
        print(f"\n[3/4] LLM eşleştirme yapılıyor...")
        match_stats = run_llm_matcher(config_id, store_id)
        print(f"  → {match_stats['matched']} eşleşti, {match_stats['unmatched']} eşleşmedi")

        # ADIM 4 — Review tablosuna import
        print(f"\n[4/4] Yorumlar Review tablosuna aktarılıyor...")
        import_stats = run_review_importer(config_id, store_id)
        print(f"  → {import_stats['imported']} yorum import edildi")

        update_queue_status(queue_id, "done")

        print(f"\n{'='*60}")
        print(f"✅ İŞ TAMAMLANDI — queue_id: {queue_id}")
        print(f"   Ürün sayısı  : {len(products)}")
        print(f"   Yorum scrape : {scrape_stats['total_saved']} kaydedildi")
        print(f"   LLM eşleşme : {match_stats['matched']} eşleşti / {match_stats['unmatched']} eşleşmedi")
        print(f"   Review import: {import_stats['imported']} yorum eklendi")
        print(f"{'='*60}\n")

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ İş başarısız — {error_msg}")
        update_queue_status(queue_id, "failed", error_log=error_msg)


async def worker():
    print("🔄 Worker başlatıldı, kuyruk dinleniyor...")

    while True:
        try:
            # Önce priority:high işlere bak
            jobs = get_pending_queue(priority="high")

            if not jobs:
                # Sonra normal işlere bak
                jobs = get_pending_queue(priority="normal")

            if jobs:
                job = dict(jobs[0])
                await process_job(job)
            else:
                print(f"⏳ Kuyruk boş, {POLL_INTERVAL}s bekleniyor...")

        except Exception as e:
            print(f"❌ Worker hatası: {e}")

        await asyncio.sleep(POLL_INTERVAL)


def run_worker():
    asyncio.run(worker())


if __name__ == "__main__":
    run_worker()