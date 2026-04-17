from db import get_connection
from datetime import datetime

def run(config_id: str, store_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()

    # Eşleşmiş ama henüz import edilmemiş yorumları al
    cur.execute("""
        SELECT 
            tr.id as trendyol_review_id,
            tr."trendyolId",
            tr.rate,
            tr.comment,
            tr."userFullName",
            tr."productSize",
            tr.trusted,
            tr."createdAt" as trendyol_created_at,
            tp."contentId",
            tp."productName",
            tp."ikasProductId",
            ip."productId" as ikas_product_id,
            ip.slug as product_slug
        FROM "TrendyolReview" tr
        JOIN "TrendyolProduct" tp ON tp.id = tr."trendyolProductId"
        JOIN "IkasProduct" ip ON ip.id = tp."ikasProductId"
        WHERE tp."configId" = %s
          AND tr."importedAt" IS NULL
          AND tp."ikasProductId" IS NOT NULL
    """, (config_id,))

    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    if not rows:
        print("ℹ️ Import edilecek yorum yok")
        return {"imported": 0, "skipped": 0}

    print(f"📥 {len(rows)} yorum import edilecek")

    imported = 0
    skipped = 0

    conn = get_connection()
    cur = conn.cursor()

    for row in rows:
        try:
            # Medya URL'lerini al
            cur.execute("""
                SELECT url FROM "TrendyolReviewMedia"
                WHERE "reviewId" = %s
            """, (row["trendyol_review_id"],))
            media_rows = cur.fetchall()
            media_urls = [m["url"] for m in media_rows if m["url"]]

            # Timestamp → datetime
            created_at = row["trendyol_created_at"]
            if isinstance(created_at, int):
                created_at = datetime.fromtimestamp(created_at / 1000)

            # customerName formatla
            customer_name = row["userFullName"] or "Anonim"

            # Review tablosuna ekle
            cur.execute("""
                INSERT INTO "Review"
                    (id, "storeId", "productId", "productName", "productSlug",
                     "customerName", rating, body, status, source,
                     "isVerified", "mediaUrls", "createdAt", "updatedAt")
                VALUES
                    (gen_random_uuid(), %s, %s, %s, %s,
                     %s, %s, %s, 'approved', 'trendyol',
                     true, %s, %s, NOW())
                RETURNING id
            """, (
                store_id,
                row["ikas_product_id"],
                row["productName"],
                row["product_slug"],
                customer_name,
                row["rate"],
                row["comment"],
                media_urls,
                created_at,
            ))

            review_row = cur.fetchone()
            if not review_row:
                skipped += 1
                continue

            review_id = review_row["id"]

            # TrendyolReview'u işaretле
            cur.execute("""
                UPDATE "TrendyolReview"
                SET "importedAt" = NOW(), "reviewId" = %s
                WHERE id = %s
            """, (review_id, row["trendyol_review_id"]))

            imported += 1

        except Exception as e:
            print(f"⚠️ Import hatası {row.get('trendyolId')}: {e}")
            skipped += 1
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ {imported} yorum import edildi, {skipped} atlandı")
    return {"imported": imported, "skipped": skipped}