from db import get_connection
from datetime import datetime
import json


def get_next_job():
    """
    Kuyruktan bir sonraki pending işi al ve running yap
    scheduledAt <= NOW() kontrolü ile zamanı gelmemiş jobları atla
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT 
                sq.id,
                sq."storeId" as store_id,
                sq."configId" as config_id,
                sq."scrapeType" as scrape_type,
                tc."sellerId" as seller_id
            FROM "ScrapeQueue" sq
            JOIN "TrendyolConfig" tc ON tc.id = sq."configId"
            WHERE sq.status = 'pending'
              AND sq."scheduledAt" <= NOW()
            ORDER BY 
                CASE sq.priority
                    WHEN 'high' THEN 1
                    WHEN 'normal' THEN 2
                    ELSE 3
                END,
                sq."scheduledAt" ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)

        job = cur.fetchone()

        if not job:
            return None

        cur.execute("""
            UPDATE "ScrapeQueue"
            SET status = 'running', "startedAt" = NOW(), "updatedAt" = NOW()
            WHERE id = %s
        """, (job["id"],))

        conn.commit()
        return dict(job)

    except Exception as e:
        conn.rollback()
        print(f"❌ Queue fetch hatası: {e}")
        return None
    finally:
        cur.close()
        conn.close()


def mark_job_completed(queue_id: str, runtime_log: dict = None):
    """
    İşi completed olarak işaretle
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE "ScrapeQueue"
            SET 
                status = 'done',
                "finishedAt" = NOW(),
                "runTimeLog" = %s,
                "updatedAt" = NOW()
            WHERE id = %s
        """, (json.dumps(runtime_log) if runtime_log else None, queue_id))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"❌ Job completion hatası: {e}")
    finally:
        cur.close()
        conn.close()


def mark_job_failed(queue_id: str, error_message: str):
    """
    İşi failed olarak işaretle
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE "ScrapeQueue"
            SET 
                status = 'failed',
                "finishedAt" = NOW(),
                "errorLog" = %s,
                "updatedAt" = NOW()
            WHERE id = %s
        """, (error_message[:1000], queue_id))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"❌ Job failure marking hatası: {e}")
    finally:
        cur.close()
        conn.close()


def add_job(config_id: str, store_id: str, priority: str = "normal", delay_minutes: int = 0):
    """
    Manuel iş ekleme
    delay_minutes: kaç dakika sonra başlasın
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO "ScrapeQueue" (
                id,
                "storeId",
                "configId",
                "scrapeType",
                status,
                priority,
                "scheduledAt",
                "createdAt",
                "updatedAt"
            )
            VALUES (
                gen_random_uuid(),
                %s, %s,
                'update',
                'pending',
                %s,
                NOW() + %s * INTERVAL '1 minute',
                NOW(),
                NOW()
            )
            RETURNING id
        """, (store_id, config_id, priority, delay_minutes))

        result = cur.fetchone()
        conn.commit()

        return result["id"] if result else None

    except Exception as e:
        conn.rollback()
        print(f"❌ Job ekleme hatası: {e}")
        return None
    finally:
        cur.close()
        conn.close()
