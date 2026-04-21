import requests
import base64



def get_auth_header():
    credentials = f"{API_KEY}:{API_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"

def test_fetch():
    headers = {
        "Authorization": get_auth_header(),
        "Content-Type": "application/json",
    }

    url = (
        f"https://apigw.trendyol.com/integration/product/sellers/{SELLER_ID}/products"
        f"?supplierId={SELLER_ID}&size=10&page=0"
    )
    
    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()
    products = data.get("content", [])

    print(f"İlk 10 ürün:\n")
    for p in products:
        content_id = p.get('productContentId')
        title = p.get('title', '—')[:60]
        images = p.get('images', [])
        image_url = images[0] if images else '—'
        product_url = f"https://www.trendyol.com/ty/ty-p-{content_id}"
        review_url = f"https://www.trendyol.com/ty/ty-p-{content_id}/yorumlar"

        print(f"  contentId : {content_id}")
        print(f"  Ürün adı  : {title}")
        print(f"  Ürün URL  : {product_url}")
        print(f"  Yorum URL : {review_url}")
        print(f"  Görsel    : {image_url}")
        print()

test_fetch()
