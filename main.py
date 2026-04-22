import asyncio
import nest_asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import psutil
import time
import traceback
from queue_manager import get_next_job, mark_job_completed, mark_job_failed
import review_scraper
from telegram_notifier import notify_service_start, notify_service_crash, notify_error, notify_success

nest_asyncio.apply()

worker_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_running
    worker_running = True
    
    # Servis başlangıç bildirimi
    notify_service_start()
    
    # Worker'ı başlat
    asyncio.create_task(worker_loop())
    print("✅ Worker başlatıldı")
    
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
                # CPU ve RAM kullanımı
                cpu = psutil.cpu_percent(interval=1)
                mem = psutil.virtual_memory()
                mem_mb = round(mem.used / 1024 / 1024)
                
                print(f"⏳ Kuyruk boş, 30s bekleniyor... [CPU:{cpu}% MEM:{mem_mb}MB]")
                await asyncio.sleep(30)
                consecutive_errors = 0  # Kuyruk boş sayılmaz
                continue
            
            # İş bilgileri
            queue_id = job["id"]
            config_id = job["config_id"]
            seller_id = job["seller_id"]
            store_id = job["store_id"]
            
            job_info = {
                "queue_id": queue_id,
                "config_id": config_id,
                "seller_id": seller_id,
                "store_id": store_id,
            }
            
            cpu = psutil.cpu_percent(interval=0)
            mem = psutil.virtual_memory()
            mem_mb = round(mem.used / 1024 / 1024)
            mem_total_mb = round(mem.total / 1024 / 1024)
            
            print(f"[{time.strftime('%H:%M:%S')}] [CPU:{cpu}% MEM:{mem_mb}MB/{mem_total_mb}MB] 🚀 İş başladı — queue_id: {queue_id}")
            print(f"[{time.strftime('%H:%M:%S')}]    store_id: {store_id} | seller_id: {seller_id}")
            
            try:
                # Scraping yap
                result = await review_scraper.run(config_id, seller_id)
                
                # Başarılı
                mark_job_completed(queue_id)
                consecutive_errors = 0
                
                print(f"[{time.strftime('%H:%M:%S')}] ✅ İş tamamlandı — {result.get('total_saved')} yorum kaydedildi")
                
                # Başarı bildirimi (sadece önemli işler için)
                if result.get('total_saved', 0) > 50:
                    notify_success(result, job_info)
                
            except Exception as e:
                error_msg = str(e)
                error_trace = traceback.format_exc()
                
                print(f"[{time.strftime('%H:%M:%S')}] ❌ Hata: {error_msg}")
                print(f"[{time.strftime('%H:%M:%S')}] 📋 Traceback:\n{error_trace}")
                
                mark_job_failed(queue_id, error_msg)
                consecutive_errors += 1
                
                # Telegram bildirimi
                notify_error(error_msg, job_info)
                
                # Çok fazla ardışık hata varsa servisi durdur
                if consecutive_errors >= max_consecutive_errors:
                    crash_msg = f"Ardışık {max_consecutive_errors} hata! Servis durduruluyor."
                    print(f"💥 {crash_msg}")
                    notify_service_crash(crash_msg)
                    break
            
            await asyncio.sleep(5)
            
        except Exception as e:
            # Worker loop hatası (kritik)
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
    Sağlık kontrolü endpoint'i
    """
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    
    return {
        "status": "ok" if worker_running else "stopped",
        "worker_running": worker_running,
        "cpu_percent": cpu,
        "memory_percent": mem.percent,
        "memory_used_mb": round(mem.used / 1024 / 1024),
        "memory_total_mb": round(mem.total / 1024 / 1024),
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