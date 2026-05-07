import asyncio
import nest_asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import psutil
import time
import traceback
from datetime import datetime
from queue_manager import get_next_job, mark_job_completed, mark_job_failed
import review_scraper
from telegram_notifier import notify_service_start, notify_service_crash, notify_error, notify_success
from db import get_connection

nest_asyncio.apply()

worker_running = False


def schedule_all_stores():
    """
    Tüm aktif mağazalar için ScrapeQueue'ya job ekle
    Mağazalar arasında 10 dakika boşluk bırak
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT 
                s.id as store_id,
                tc.id as config_id,
                tc."sellerId" as seller_id
            FROM "TrendyolConfig" tc
            JOIN "Store" s ON tc."storeId" = s.id
            WHERE tc."isActive" = true
              AND s."subscriptionStatus" != 'uninstalled'
            ORDER BY s."createdAt" ASC
        """)
        stores = cur.fetchall()

        if not stores:
            print(f"[{time.strftime('%H:%M:%S')}] ℹ️ Aktif mağaza bulunamadı")
            return 0

        print(f"[{time.strftime('%H:%M:%S')}] 📋 {len(stores)} aktif mağaza bulundu, joblar ekleniyor...")

        for i, store in enumerate(stores):
            delay_minutes = i * 10
            cur.execute("""
                INSERT INTO "ScrapeQueue" (
                    id,
                    "storeId",
                    "configId",
                    "scrapeType",
                    "status",
                    "priority",
                    "scheduledAt",
                    "createdAt",
                    "updatedAt"
                ) VALUES (
                    gen_random_uuid(),
                    %s, %s,
                    'update',
                    'pending',
                    'normal',
                    NOW() + %s * INTERVAL '1 minute',
                    NOW(),
                    NOW()
                )
            """, (store["store_id"], store["config_id"], delay_minutes))

            print(f"[{time.strftime('%H:%M:%S')}]    + {store['seller_id']} → +{delay_minutes} dakika sonra")

        conn.commit()
        print(f"[{time.strftime('%H:%M:%S')}] ✅ {len(stores)} job eklendi")
        return len(stores)

    except Exception as e:
        conn.rollback()
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Schedule hatası: {e}")
        return 0
    finally:
        cur.close()
        conn.close()


async def cron_loop():
    """
    Her gece 00:00'da tüm mağazaları sıraya ekle
    """
    print(f"[{time.strftime('%H:%M:%S')}] ⏰ Cron loop başlatıldı")

    while worker_running:
        now = datetime.now()

        # Gece 00:00'a kaç saniye var?
        seconds_until_midnight = (
            (24 - now.hour - 1) * 3600
            + (60 - now.minute - 1) * 60
            + (60 - now.second)
        )

        hours_left = seconds_until_midnight // 3600
        minutes_left = (seconds_until_midnight % 3600) // 60

        print(f"[{time.strftime('%H:%M:%S')}] ⏰ Bir sonraki cron: gece 00:00 ({hours_left}s {minutes_left}dk sonra)")

        # Gece 00:00'a kadar bekle
        await asyncio.sleep(seconds_until_midnight)

        # Jobları ekle
        print(f"[{time.strftime('%H:%M:%S')}] 🌙 Gece cron başladı — tüm mağazalar sıraya alınıyor")
        count = schedule_all_stores()
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Cron tamamlandı — {count} mağaza sıraya alındı")

        # 61 saniye bekle (aynı gece tekrar tetiklenmesin)
        await asyncio.sleep(61)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_running
    worker_running = True

    notify_service_start()

    asyncio.create_task(worker_loop())
    asyncio.create_task(cron_loop())

    print("✅ Worker başlatıldı")
    print("✅ Cron loop başlatıldı")

    yield

    worker_running = False


app = FastAPI(lifespan=lifespan)


async def worker_loop():
    """
    Sürekli kuyruktan iş al ve işle
    """
    print(f"[{time.strftime('%H:%M:%S')}] 🔄 Worker başlatıldı, kuyruk dinleniyor...")

    consecutive_errors = 0
    max_consecutive_errors = 5

    while worker_running:
        try:
            job = get_next_job()

            if not job:
                cpu = psutil.cpu_percent(interval=1)
                mem = psutil.virtual_memory()
                mem_mb = round(mem.used / 1024 / 1024)

                print(f"⏳ Kuyruk boş, 30s bekleniyor... [CPU:{cpu}% MEM:{mem_mb}MB]")
                await asyncio.sleep(30)
                consecutive_errors = 0
                continue

            queue_id = job["id"]
            config_id = job["config_id"]
            seller_id = job["seller_id"]
            store_id = job["store_id"]
            scrape_type = job.get("scrape_type", "update")

            job_info = {
                "queue_id": queue_id,
                "config_id": config_id,
                "seller_id": seller_id,
                "store_id": store_id,
                "scrape_type": scrape_type,
            }

            cpu = psutil.cpu_percent(interval=0)
            mem = psutil.virtual_memory()
            mem_mb = round(mem.used / 1024 / 1024)
            mem_total_mb = round(mem.total / 1024 / 1024)

            print(f"[{time.strftime('%H:%M:%S')}] [CPU:{cpu}% MEM:{mem_mb}MB/{mem_total_mb}MB] 🚀 İş başladı — queue_id: {queue_id}")
            print(f"[{time.strftime('%H:%M:%S')}]    store_id: {store_id} | seller_id: {seller_id} | type: {scrape_type}")

            try:
                result = await review_scraper.run(config_id, seller_id, scrape_type)
                runtime_log = result.get('runtime_log')

                mark_job_completed(queue_id, runtime_log)
                consecutive_errors = 0

                print(f"[{time.strftime('%H:%M:%S')}] ✅ İş tamamlandı — {result.get('total_saved')} yorum kaydedildi")

                if runtime_log:
                    scraped = runtime_log.get('scrapedData', {})
                    auto_pub = runtime_log.get('autoPublished', {})
                    print(f"[{time.strftime('%H:%M:%S')}]    📊 Çekilen: {scraped.get('totalScraped')} | Yeni: {scraped.get('newReviews')} | Duplicate: {scraped.get('duplicateReviews')}")
                    print(f"[{time.strftime('%H:%M:%S')}]    🚀 Auto-publish: {auto_pub.get('publishedReviews')} yayınlandı | {auto_pub.get('skippedUnmatched')} beklemede")

                if result.get('total_saved', 0) > 50:
                    notify_success(result, job_info)

            except Exception as e:
                error_msg = str(e)
                error_trace = traceback.format_exc()

                print(f"[{time.strftime('%H:%M:%S')}] ❌ Hata: {error_msg}")
                print(f"[{time.strftime('%H:%M:%S')}] 📋 Traceback:\n{error_trace}")

                mark_job_failed(queue_id, error_msg)
                consecutive_errors += 1

                notify_error(error_msg, job_info)

                if consecutive_errors >= max_consecutive_errors:
                    crash_msg = f"Ardışık {max_consecutive_errors} hata! Servis durduruluyor."
                    print(f"💥 {crash_msg}")
                    notify_service_crash(crash_msg)
                    break

            await asyncio.sleep(5)

        except Exception as e:
            error_msg = f"Worker loop kritik hatası: {str(e)}"
            error_trace = traceback.format_exc()

            print(f"💥 {error_msg}")
            print(f"📋 Traceback:\n{error_trace}")

            notify_service_crash(f"{error_msg}\n\n{error_trace[:500]}")
            consecutive_errors += 1

            if consecutive_errors >= max_consecutive_errors:
                print(f"💥 Servis durduruluyor...")
                break

            await asyncio.sleep(10)

@app.get("/health")
async def health():
    """
    Healthcheck - Worker gerçekten çalışıyor mu?
    """
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    
    # Son 10 dakikada job activity var mı?
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Son 10 dakikadaki aktivite
        cur.execute("""
            SELECT COUNT(*) as count
            FROM "ScrapeQueue"
            WHERE "updatedAt" > NOW() - INTERVAL '10 minutes'
        """)
        recent_activity = cur.fetchone()["count"]
        
        # Zamanı gelmiş pending job'lar
        cur.execute("""
            SELECT COUNT(*) as count
            FROM "ScrapeQueue"
            WHERE status = 'pending'
              AND "scheduledAt" <= NOW()
        """)
        pending_ready = cur.fetchone()["pending_count"]
        
        cur.close()
        conn.close()
        
        # Worker çalışıyor + (son 10dk aktivite var VEYA bekleyen job yok) = SAĞLIKLI
        is_healthy = worker_running and (recent_activity > 0 or pending_ready == 0)
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "worker_running": worker_running,
            "recent_activity": recent_activity,
            "pending_ready_jobs": pending_ready,
            "cpu_percent": cpu,
            "memory_percent": mem.percent,
            "memory_used_mb": round(mem.used / 1024 / 1024),
            "memory_total_mb": round(mem.total / 1024 / 1024),
        }
        
    except Exception as e:
        return {
            "status": "error",
            "worker_running": worker_running,
            "error": str(e),
            "cpu_percent": cpu,
            "memory_percent": mem.percent,
        }


@app.get("/")
async def root():
    return {
        "service": "Trendyol Review Scraper",
        "version": "2.0.0",
        "status": "running" if worker_running else "stopped"
    }


if __name__ == "__main__":
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
    except Exception as e:
        error_msg = f"Servis başlatma hatası: {str(e)}"
        print(f"💥 {error_msg}")
        notify_service_crash(error_msg)
