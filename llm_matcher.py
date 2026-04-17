import os
import json
from difflib import SequenceMatcher
from openai import OpenAI
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

openai_client = OpenAI(api_key=OPENAI_API_KEY)
deepseek_client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

PARALLEL_WORKERS = 20
CONFIDENCE_THRESHOLD = 90

SYSTEM_PROMPT = """Sen bir Türk e-ticaret ürün eşleştirme uzmanısın.
Sana bir Trendyol ürün ismi ve bir ikas mağaza ürün listesi verilecek.

GÖREV: Bu Trendyol ürünü, ikas listesindeki ürünlerden biriyle GERÇEKTEN aynı ürün mü?

EŞLEŞTIRME KURALLARI:
1. Aynı ürün kategorisi olmalı (mont=mont, tulum=tulum, elbise=elbise)
2. Yazım farkları kabul edilir: büyük/küçük harf, Türkçe karakter, kısaltma
3. Renk/beden farkları kabul edilir (aynı ürünün varyantları)
4. Marka/model aynı olmalı
5. Tamamen farklı ürün kategorilerini ASLA eşleştirme

ÖRNEKLER:
✅ DOĞRU EŞLEŞTİRMELER:
- "Baby Welsoft Mont" = "baby welsoft mont" (büyük/küçük harf farkı)
- "Kapüşonlu Bebek Tulum Mavi" = "Kapüşonlu Bebek Tulum" (renk farkı)
- "Mont Bebek Welsoft Astarlı" = "Bebek Welsoft Astarlı Mont" (kelime sırası)
- "Sherpa Mont Kız Bebek" = "Kız Bebek Sherpa Mont" (kelime sırası)

❌ YANLIŞ EŞLEŞTİRMELER:
- "Baby Tulum" ≠ "Kapüşonlu Mont" (farklı kategori)
- "Bebek Elbise" ≠ "Bebek Yelek" (farklı ürün)
- "Jakarlı Tulum" ≠ "Welsoft Mont" (farklı kategori)
- "Bebek Pijama" ≠ "Bebek Mont" (farklı kategori)

Emin değilsen MUTLAKA null döndür. Zorla eşleştirme yapma.

Sadece JSON formatında yanıt ver:
{
  "ikasProductId": "uuid veya null",
  "ikasProductName": "ürün adı veya null",
  "confidence": 0-100,
  "reasoning": "kısa Türkçe açıklama"
}"""


def normalize_text(text: str) -> str:
    return text.lower().strip() \
        .replace('ğ', 'g').replace('ü', 'u').replace('ş', 's') \
        .replace('ı', 'i').replace('ö', 'o').replace('ç', 'c') \
        .replace('Ğ', 'g').replace('Ü', 'u').replace('Ş', 's') \
        .replace('İ', 'i').replace('Ö', 'o').replace('Ç', 'c')


def fuzzy_match_all(trendyol_products: list, ikas_products: list, threshold: float = 0.95):
    matched = []
    unmatched = []

    ikas_map = {ip["name"].lower().strip(): ip for ip in ikas_products}
    ikas_map_normalized = {normalize_text(ip["name"]): ip for ip in ikas_products}

    for tp in trendyol_products:
        tp_name = tp["productName"].lower().strip()
        tp_normalized = normalize_text(tp["productName"])

        # Birebir eşleşme
        if tp_name in ikas_map:
            ip = ikas_map[tp_name]
            matched.append({
                "contentId": tp["contentId"],
                "trendyolName": tp["productName"],
                "ikasProductId": ip["productId"],
                "ikasName": ip["name"],
                "confidence": 100,
                "ikas_internal_id": ip["id"],
            })
            print(f"  ✅ Birebir: {tp['productName'][:50]}")
            continue

        # Normalize birebir eşleşme
        if tp_normalized in ikas_map_normalized:
            ip = ikas_map_normalized[tp_normalized]
            matched.append({
                "contentId": tp["contentId"],
                "trendyolName": tp["productName"],
                "ikasProductId": ip["productId"],
                "ikasName": ip["name"],
                "confidence": 95,
                "ikas_internal_id": ip["id"],
            })
            print(f"  ✅ Normalize: {tp['productName'][:50]} → {ip['name'][:50]}")
            continue

        # Fuzzy eşleşme
        best_ratio = 0
        best_ikas = None

        for ip in ikas_products:
            ratio = SequenceMatcher(None, tp_normalized, normalize_text(ip["name"])).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_ikas = ip

        if best_ratio >= threshold:
            matched.append({
                "contentId": tp["contentId"],
                "trendyolName": tp["productName"],
                "ikasProductId": best_ikas["productId"],
                "ikasName": best_ikas["name"],
                "confidence": int(best_ratio * 100),
                "ikas_internal_id": best_ikas["id"],
            })
            print(f"  🎯 Fuzzy: {tp['productName'][:50]} → {best_ikas['name'][:50]} ({best_ratio:.2f})")
        else:
            unmatched.append(tp)

    return matched, unmatched


def _call_deepseek(prompt: str) -> dict:
    try:
        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=1000,
        )
        content = response.choices[0].message.content.strip()
        content = content.replace("```json", "").replace("```", "").strip()
        return json.loads(content)
    except Exception as e:
        print(f"❌ DeepSeek hatası: {e}")
        return None


def _call_openai(prompt: str) -> dict:
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=1000,
        )
        content = response.choices[0].message.content.strip()
        content = content.replace("```json", "").replace("```", "").strip()
        return json.loads(content)
    except Exception as e:
        print(f"❌ OpenAI hatası: {e}")
        return None


def _match_single_product(tp: dict, ikas_products: list) -> dict:
    content_id = tp["contentId"]
    product_name = tp["productName"]

    ikas_list_str = "\n".join([
        f"{i+1}. ID: {ip['productId']}, İsim: \"{ip['name']}\""
        for i, ip in enumerate(ikas_products)
    ])

    prompt = f"""Trendyol ürünü: "{product_name}"

ikas mağaza ürünleri:
{ikas_list_str}

Bu Trendyol ürünü yukarıdaki listede var mı?
Yazım farkı, renk/beden varyantı olabilir ama kategori aynı olmalı.
Emin değilsen null döndür."""

    # Önce DeepSeek dene
    result = _call_deepseek(prompt)

    # DeepSeek başarısız → OpenAI
    if not result:
        result = _call_openai(prompt)

    if not result:
        return {"contentId": content_id, "matched": False}

    ikas_product_id = result.get("ikasProductId")
    confidence = result.get("confidence", 0)

    if not ikas_product_id or ikas_product_id == "null" or confidence < CONFIDENCE_THRESHOLD:
        return {"contentId": content_id, "matched": False}

    ikas_row = next((p for p in ikas_products if p["productId"] == ikas_product_id), None)
    if not ikas_row:
        return {"contentId": content_id, "matched": False}

    print(f"  ✅ LLM: {product_name[:40]} → {result.get('ikasProductName', '')[:40]} ({confidence}) | {result.get('reasoning', '')}")

    return {
        "contentId": content_id,
        "matched": True,
        "ikas_internal_id": ikas_row["id"],
        "ikasProductId": ikas_product_id,
        "trendyolName": product_name,
        "ikasName": result.get("ikasProductName"),
        "confidence": confidence,
    }


def run(config_id: str, store_id: str) -> dict:
    from db import get_connection

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

    # ADIM 1 — Birebir + Normalize + Fuzzy
    print(f"\n🎯 Hızlı eşleştirme başlıyor...")
    fuzzy_matched, llm_needed = fuzzy_match_all(trendyol_products, ikas_products)
    print(f"\n  ✅ Hızlı eşleşti: {len(fuzzy_matched)} | LLM'e gidecek: {len(llm_needed)}")

    matched = 0
    unmatched = 0

    if fuzzy_matched:
        conn = get_connection()
        cur = conn.cursor()
        for match in fuzzy_matched:
            try:
                cur.execute("""
                    UPDATE "TrendyolProduct"
                    SET "ikasProductId" = %s, "updatedAt" = NOW()
                    WHERE "configId" = %s AND "contentId" = %s
                """, (match["ikas_internal_id"], config_id, match["contentId"]))
                matched += 1
            except Exception as e:
                print(f"  ❌ Kayıt hatası: {e}")
                unmatched += 1
        conn.commit()
        cur.close()
        conn.close()

    # ADIM 2 — Paralel LLM (tek tek, 20 paralel)
    if llm_needed:
        print(f"\n🤖 LLM match başlıyor — {len(llm_needed)} ürün ({PARALLEL_WORKERS} paralel)...")

        llm_results = []
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {
                executor.submit(_match_single_product, tp, ikas_products): tp
                for tp in llm_needed
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    llm_results.append(result)
                except Exception as e:
                    print(f"❌ Worker hatası: {e}")

        conn = get_connection()
        cur = conn.cursor()

        for result in llm_results:
            if not result.get("matched"):
                unmatched += 1
                continue

            try:
                cur.execute("""
                    UPDATE "TrendyolProduct"
                    SET "ikasProductId" = %s, "updatedAt" = NOW()
                    WHERE "configId" = %s AND "contentId" = %s
                """, (result["ikas_internal_id"], config_id, result["contentId"]))
                matched += 1
            except Exception as e:
                print(f"  ❌ Kayıt hatası: {e}")
                unmatched += 1

        conn.commit()
        cur.close()
        conn.close()

    print(f"\n✅ Toplam: {matched} eşleşti, {unmatched} eşleşmedi")
    return {"matched": matched, "unmatched": unmatched}


    