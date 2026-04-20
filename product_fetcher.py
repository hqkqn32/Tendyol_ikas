import requests
import base64
from db import get_connection

def get_auth_header(api_key: str, api_secret: str) -> str:
    credentials = f"{api_key}:{api_secret}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"

def fetch_all_content_ids(seller_id: str, api_key: str, api_secret: str) -> list:
    headers = {
        "Authorization": get_auth_header(api_key, api_secret),
        "Content-Type": "application/json",
    }

    content_id_map = {}  # contentId → {productName, imageUrl}
    page = 0
    total_pages = 1

    while page < total_pages:
        url = (
            f"https://apigw.trendyol.com/integration/product/sellers/{seller_id}/products"
            f"?supplierId={seller_id}&size=50&page={page}"
        )
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                print(f"❌ API hatası sayfa {page}: {response.status_code}")
                break

            data = response.json()
            total_pages = data.get("totalPages", 1)
            products = data.get("content", [])

            for p in products:
                if not p.get("onSale", False):
                    continue
                cid = str(p.get("productContentId", ""))
                if cid and cid not in content_id_map:
                    images = p.get("images", [])
                    content_id_map[cid] = {
                        "contentId": cid,
                        "productName": p.get("title", ""),
                        "imageUrl": images[0] if images else None,
                    }

            print(f"📦 Sayfa {page + 1}/{total_pages} — toplam {len(content_id_map)} unique ürün")
            page += 1

        except Exception as e:
            print(f"❌ Hata sayfa {page}: {e}")
            break

    return list(content_id_map.values())

def save_trendyol_products(config_id: str, products: list) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    saved = 0
    updated = 0

    for p in products:
        try:
            # imageUrl dict gelebilir, string'e çevir
            image_url = p.get("imageUrl")
            if isinstance(image_url, dict):
                image_url = image_url.get("url") or None
            elif isinstance(image_url, list):
                image_url = image_url[0] if image_url else None

            cur.execute("""
                INSERT INTO "TrendyolProduct"
                    (id, "configId", "contentId", "productName", "imageUrl", "createdAt", "updatedAt")
                VALUES (gen_random_uuid(), %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT ("configId", "contentId") DO UPDATE SET
                    "productName" = EXCLUDED."productName",
                    "imageUrl" = EXCLUDED."imageUrl",
                    "updatedAt" = NOW()
                RETURNING (xmax = 0) AS inserted
            """, (
                config_id,
                str(p.get("contentId")),
                p.get("productName", ""),
                image_url,
            ))
            row = cur.fetchone()
            if row and row["inserted"]:
                saved += 1
            else:
                updated += 1
        except Exception as e:
            print(f"⚠️ Ürün kayıt hatası {p.get('contentId')}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {saved} yeni ürün kaydedildi, {updated} güncellendi")
    return {"saved": saved, "updated": updated}

def run(config_id: str, seller_id: str, api_key: str, api_secret: str) -> list:
    print(f"🚀 Ürün listesi çekiliyor — seller: {seller_id}")
    products = fetch_all_content_ids(seller_id, api_key, api_secret)
    print(f"✅ Toplam {len(products)} unique ürün bulundu")
    save_trendyol_products(config_id, products)
    return products