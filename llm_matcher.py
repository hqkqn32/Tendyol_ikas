from difflib import SequenceMatcher
from db import get_connection


def normalize_text(text: str) -> str:
    return text.lower().strip() \
        .replace('ğ', 'g').replace('ü', 'u').replace('ş', 's') \
        .replace('ı', 'i').replace('ö', 'o').replace('ç', 'c') \
        .replace('Ğ', 'g').replace('Ü', 'u').replace('Ş', 's') \
        .replace('İ', 'i').replace('Ö', 'o').replace('Ç', 'c')


def match_products(trendyol_products: list, ikas_products: list, threshold: float = 0.92) -> list:
    matched = []
    ikas_map = {ip["name"].lower().strip(): ip for ip in ikas_products}
    ikas_map_normalized = {normalize_text(ip["name"]): ip for ip in ikas_products}

    for tp in trendyol_products:
        tp_name = tp["productName"].lower().strip()
        tp_normalized = normalize_text(tp["productName"])

        # Birebir
        if tp_name in ikas_map:
            ip = ikas_map[tp_name]
            matched.append({**tp, "ikas_internal_id": ip["id"], "confidence": 100})
            print(f"  ✅ Birebir: {tp['productName'][:50]}")
            continue

        # Normalize birebir
        if tp_normalized in ikas_map_normalized:
            ip = ikas_map_normalized[tp_normalized]
            matched.append({**tp, "ikas_internal_id": ip["id"], "confidence": 95})
            print(f"  ✅ Normalize: {tp['productName'][:50]} → {ip['name'][:50]}")
            continue

        # Fuzzy
        best_ratio = 0
        best_ikas = None
        for ip in ikas_products:
            ratio = SequenceMatcher(None, tp_normalized, normalize_text(ip["name"])).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_ikas = ip

        if best_ratio >= threshold:
            matched.append({**tp, "ikas_internal_id": best_ikas["id"], "confidence": int(best_ratio * 100)})
            print(f"  🎯 Fuzzy: {tp['productName'][:50]} → {best_ikas['name'][:50]} ({best_ratio:.2f})")

    return matched


def run(config_id: str, store_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, "contentId", "productName"
        FROM "TrendyolProduct"
        WHERE "configId" = %s AND "ikasProductId" IS NULL
    """, (config_id,))
    trendyol_products = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT id, "productId", name, slug
        FROM "IkasProduct"
        WHERE "storeId" = %s
    """, (store_id,))
    ikas_products = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()

    if not trendyol_products:
        print("ℹ️ Eşleştirilecek ürün yok")
        return {"matched": 0, "unmatched": 0}

    if not ikas_products:
        print("❌ ikas ürünleri bulunamadı")
        return {"matched": 0, "unmatched": len(trendyol_products)}

    print(f"🔍 {len(trendyol_products)} Trendyol ürünü, {len(ikas_products)} ikas ürünü")

    matched = match_products(trendyol_products, ikas_products)
    unmatched = len(trendyol_products) - len(matched)

    if matched:
        conn = get_connection()
        cur = conn.cursor()
        for match in matched:
            try:
                cur.execute("""
                    UPDATE "TrendyolProduct"
                    SET "ikasProductId" = %s, "updatedAt" = NOW()
                    WHERE "configId" = %s AND "contentId" = %s
                """, (match["ikas_internal_id"], config_id, match["contentId"]))
            except Exception as e:
                print(f"  ❌ Kayıt hatası: {e}")
        conn.commit()
        cur.close()
        conn.close()

    print(f"\n✅ Toplam: {len(matched)} eşleşti, {unmatched} eşleşmedi")
    return {"matched": len(matched), "unmatched": unmatched}