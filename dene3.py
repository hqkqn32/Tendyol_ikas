import json
with open("reviews_full.json", encoding="utf-8") as f:
    data = json.load(f)
print(f"Unique ürün: {len(data)}")