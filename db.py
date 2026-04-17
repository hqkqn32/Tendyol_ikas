import os
import time
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection(retries=3, delay=5):
    for i in range(retries):
        try:
            return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            if i < retries - 1:
                print(f"⚠️ DB bağlantı hatası, {delay}s sonra tekrar ({i+1}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise

def get_pending_queue(priority: str = None):
    conn = get_connection()
    cur = conn.cursor()
    if priority:
        cur.execute("""
            SELECT sq.*, tc."sellerId", tc."apiKey", tc."apiSecret", tc."storeId" as "tcStoreId"
            FROM "ScrapeQueue" sq
            JOIN "TrendyolConfig" tc ON tc.id = sq."configId"
            WHERE sq.status = 'pending' AND sq.priority = %s
            ORDER BY sq."createdAt" ASC
        """, (priority,))
    else:
        cur.execute("""
            SELECT sq.*, tc."sellerId", tc."apiKey", tc."apiSecret", tc."storeId" as "tcStoreId"
            FROM "ScrapeQueue" sq
            JOIN "TrendyolConfig" tc ON tc.id = sq."configId"
            WHERE sq.status = 'pending'
            ORDER BY sq.priority DESC, sq."createdAt" ASC
        """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def update_queue_status(queue_id: str, status: str, error_log: str = None):
    conn = get_connection()
    cur = conn.cursor()
    if status == 'running':
        cur.execute("""
            UPDATE "ScrapeQueue" SET status = %s, "startedAt" = NOW(), "updatedAt" = NOW()
            WHERE id = %s
        """, (status, queue_id))
    elif status in ('done', 'failed'):
        cur.execute("""
            UPDATE "ScrapeQueue" SET status = %s, "finishedAt" = NOW(), "updatedAt" = NOW(), "errorLog" = %s
            WHERE id = %s
        """, (status, error_log, queue_id))
    else:
        cur.execute("""
            UPDATE "ScrapeQueue" SET status = %s, "updatedAt" = NOW()
            WHERE id = %s
        """, (status, queue_id))
    conn.commit()
    cur.close()
    conn.close()

def get_ikas_products(store_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, "productId", name, slug
        FROM "IkasProduct"
        WHERE "storeId" = %s
    """, (store_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_store_by_id(store_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM "Store" WHERE id = %s', (store_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row