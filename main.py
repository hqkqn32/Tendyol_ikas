import asyncio
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import get_connection, update_queue_status
from queue_manager import worker
import uvicorn

app = FastAPI(title="Trendyol YorumKit Scraper")

# Worker thread'i global tut
worker_task = None

# ─── MODELS ───────────────────────────────────────────

class TriggerRequest(BaseModel):
    store_id: str

# ─── STARTUP ──────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    global worker_task
    worker_task = asyncio.create_task(worker())
    print("✅ Worker başlatıldı")

# ─── ENDPOINTS ────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/scrape/trigger")
def trigger_scrape(body: TriggerRequest):
    conn = get_connection()
    cur = conn.cursor()

    # Store var mı kontrol et
    cur.execute('SELECT id FROM "Store" WHERE id = %s', (body.store_id,))
    store = cur.fetchone()
    if not store:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Store bulunamadı")

    # TrendyolConfig var mı kontrol et
    cur.execute("""
        SELECT id FROM "TrendyolConfig"
        WHERE "storeId" = %s AND "isActive" = true
    """, (body.store_id,))
    config = cur.fetchone()
    if not config:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="TrendyolConfig bulunamadı")

    # Zaten pending/running iş var mı
    cur.execute("""
        SELECT id FROM "ScrapeQueue"
        WHERE "storeId" = %s AND status IN ('pending', 'running')
    """, (body.store_id,))
    existing = cur.fetchone()
    if existing:
        cur.close()
        conn.close()
        return {"message": "Zaten kuyrukta bir iş var", "queue_id": existing["id"]}

    # Kuyruğa ekle
    cur.execute("""
        INSERT INTO "ScrapeQueue"
            (id, "storeId", "configId", status, priority, "createdAt", "updatedAt")
        VALUES (gen_random_uuid(), %s, %s, 'pending', 'high', NOW(), NOW())
        RETURNING id
    """, (body.store_id, config["id"]))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return {"message": "Scrape kuyruğa eklendi", "queue_id": row["id"]}


@app.get("/scrape/status/{store_id}")
def scrape_status(store_id: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, status, priority, "scheduledAt", "startedAt", "finishedAt", "errorLog"
        FROM "ScrapeQueue"
        WHERE "storeId" = %s
        ORDER BY "createdAt" DESC
        LIMIT 5
    """, (store_id,))

    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    return {"jobs": rows}


@app.post("/cron/weekly")
def cron_weekly():
    conn = get_connection()
    cur = conn.cursor()

    # Tüm aktif TrendyolConfig'leri al
    cur.execute("""
        SELECT tc.id as config_id, tc."storeId"
        FROM "TrendyolConfig" tc
        JOIN "Store" s ON s.id = tc."storeId"
        WHERE tc."isActive" = true
          AND s."subscriptionStatus" IN ('trial', 'active')
    """)
    configs = [dict(r) for r in cur.fetchall()]

    added = 0
    skipped = 0

    for config in configs:
        # Zaten pending/running iş var mı
        cur.execute("""
            SELECT id FROM "ScrapeQueue"
            WHERE "storeId" = %s AND status IN ('pending', 'running')
        """, (config["storeId"],))
        existing = cur.fetchone()

        if existing:
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO "ScrapeQueue"
                (id, "storeId", "configId", status, priority, "createdAt", "updatedAt")
            VALUES (gen_random_uuid(), %s, %s, 'pending', 'normal', NOW(), NOW())
        """, (config["storeId"], config["config_id"]))
        added += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Cron: {added} mağaza kuyruğa eklendi, {skipped} atlandı")
    return {"added": added, "skipped": skipped}


# ─── RUN ──────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)