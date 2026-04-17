import asyncio
from review_scraper import fetch_reviews_for_product

SELLER_ID = "212112"  # kendi seller_id'n

CONTENT_IDS = [
    "1107346396",
    "1133150163",
    "1093268400",
    "1093251060",
    "1094209513",
    "1094209514",
]

async def test():
    for content_id in CONTENT_IDS:
        print(f"\n{'='*50}")
        print(f"🔍 ContentId: {content_id}")

        result = await fetch_reviews_for_product(content_id, seller_id=SELLER_ID)
        reviews = result.get("reviews", [])
        summary = result.get("summary", {})

        print(f"  Toplam yorum (Trendyol): {summary.get('totalCommentCount', 0)}")
        print(f"  Bizim yorumlarımız: {len(reviews)}")

        if reviews:
            for i, r in enumerate(reviews):
                print(f"\n  [{i+1}] {r.get('userFullName')} — {r.get('rate')}⭐")
                print(f"       {str(r.get('comment', ''))[:120]}")
                media = r.get('mediaFiles', [])
                if media:
                    for m in media:
                        print(f"       📷 {m.get('url')}")

asyncio.run(test())