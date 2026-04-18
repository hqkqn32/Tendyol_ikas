import asyncio
import psutil
import time
from datetime import datetime
from db import get_connection, get_pending_queue, update_queue_status
from product_fetcher import run as run_product_fetcher
from review_scraper import run as run_review_scraper
from llm_matcher import run as run_llm_matcher
from review_importer import run as run_review_importer

POLL_INTERVAL = 30

def log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()
    mem_used = f"{mem.used // 1024 // 1024}MB/{mem.total // 1024 // 1024}MB"
    print(f"[{now}] [CPU:{cpu:.1f}% MEM:{mem_used}] {msg}", flush=True)

def log_simple(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

async def heartbeat(stop_event: asyncio.Event, job_info: dict):
    """Her 30 saniyede bir durumu logla, işi bloklamadan."""
    while not stop_event.is_set():
        await asyncio.sleep(30)
        if not stop_event.is_set():
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            mem_used = f"{mem.used // 1024 // 1024}MB/{mem.total // 1024 // 1024}MB"
            step = job_info.get("step", "?")
            detail = job_info.get("detail", "")
            print(
                f"[HEARTBEAT] CPU:{cpu:.1f}% MEM:{mem_used} | Adım: {step} | {detail}",
                flush=True
            )

async def process_job(job: dict):
    queue_id = job["id"]
    config_id = job["configId"]
    store_id = job["storeId"]
    seller_id = job["sellerId"]
    api_key = job["apiKey"]
    api_secret = job["apiSecret"]

    log(f"🚀 İş başladı — queue_id: {queue_id}")
    log(f"   store_id: {store_id} | seller_id: {seller_id}")

    update_queue_status(queue_id, "running")

    job_info = {"step": "başlıyor", "detail": ""}
    stop_event = asyncio.Event()
    heartbeat_task = asyncio.create_task(heartbeat(stop_event, job_info))

    try:
        # ADIM 1 — Ürünleri çek
        job_info["step"] = "1/4 Ürün listesi"
        job_info["detail"] = "Trendyol API'den çekiliyor..."
        log(f"\n[1/4] Ürünler çekiliyor...")
        t0 = time.time()
        products = run_product_fetcher(config_id, seller_id, api_key, api_secret)
        log(f"[1/4] ✅ {len(products)} ürün çekildi ({time.time()-t0:.1f}s)")

        if not products:
            raise Exception("Ürün listesi boş geldi")

        # ADIM 2 — Yorumları çek
        job_info["step"] = "2/4 Yorum scraping"
        job_info["detail"] = f"{len(products)} ürün işlenecek"
        log(f"\n[2/4] Yorumlar çekiliyor — {len(products)} ürün...")

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT "contentId" FROM "TrendyolProduct"
            WHERE "configId" = %s
        """, (config_id,))
        db_products = [{"contentId": r["contentId"]} for r in cur.fetchall()]
        cur.close()
        conn.close()

        log(f"[2/4] DB'den {len(db_products)} ürün alındı")
        t0 = time.time()
        scrape_stats = await run_review_scraper(config_id, seller_id, db_products)
        log(f"[2/4] ✅ {scrape_stats['total_saved']} yorum kaydedildi ({time.time()-t0:.1f}s)")

        # ADIM 3 — LLM eşleştirme
        job_info["step"] = "3/4 LLM eşleştirme"
        job_info["detail"] = "Paralel LLM çalışıyor..."
        log(f"\n[3/4] LLM eşleştirme yapılıyor...")
        t0 = time.time()
        match_stats = run_llm_matcher(config_id, store_id)
        log(f"[3/4] ✅ {match_stats['matched']} eşleşti, {match_stats['unmatched']} eşleşmedi ({time.time()-t0:.1f}s)")

        # ADIM 4 — Review import
        job_info["step"] = "4/4 Review import"
        job_info["detail"] = "Review tablosuna yazılıyor..."
        log(f"\n[4/4] Yorumlar Review tablosuna aktarılıyor...")
        t0 = time.time()
        import_stats = run_review_importer(config_id, store_id)
        log(f"[4/4] ✅ {import_stats['imported']} yorum import edildi ({time.time()-t0:.1f}s)")

        update_queue_status(queue_id, "done")

        log(f"\n{'='*60}")
        log(f"✅ İŞ TAMAMLANDI — queue_id: {queue_id}")
        log(f"   Ürün sayısı  : {len(products)}")
        log(f"   Yorum scrape : {scrape_stats['total_saved']} kaydedildi")
        log(f"   LLM eşleşme : {match_stats['matched']} eşleşti / {match_stats['unmatched']} eşleşmedi")
        log(f"   Review import: {import_stats['imported']} yorum eklendi")
        log(f"{'='*60}\n")

    except Exception as e:
        error_msg = str(e)
        log(f"❌ İş başarısız — {error_msg}")
        update_queue_status(queue_id, "failed", error_log=error_msg)
    finally:
        stop_event.set()
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


async def worker():
    log_simple("🔄 Worker başlatıldı, kuyruk dinleniyor...")
    psutil.cpu_percent(interval=None)  # ilk çağrı bazen 0 döner, ısıt

    while True:
        try:
            jobs = get_pending_queue(priority="high")
            if not jobs:
                jobs = get_pending_queue(priority="normal")

            if jobs:
                job = dict(jobs[0])
                await process_job(job)
            else:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory()
                print(
                    f"⏳ Kuyruk boş, {POLL_INTERVAL}s bekleniyor... "
                    f"[CPU:{cpu:.1f}% MEM:{mem.used//1024//1024}MB]",
                    flush=True
                )

        except Exception as e:
            log(f"❌ Worker hatası: {e}")

        await asyncio.sleep(POLL_INTERVAL)


def run_worker():
    asyncio.run(worker())


if __name__ == "__main__":
    run_worker()