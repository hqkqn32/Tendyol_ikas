import sys
sys.path.append('..')
from llm_matcher import _call_openai, _call_deepseek

test_trendyol = [
    {"contentId": "123", "productName": "Unicorn Baskılı Kız Bebek İkili Takım"},
    {"contentId": "456", "productName": "Marin Desenli Erkek Bebek Takım"},
]

test_ikas = [
    {"productId": "abc", "name": "Unicorn Baskılı Takım", "slug": "unicorn-takim"},
    {"productId": "def", "name": "Deniz Temalı Bebek Takım", "slug": "deniz-takim"},
]

import json

prompt = f"""
Trendyol ürünleri:
{json.dumps([{"contentId": p["contentId"], "name": p["productName"]} for p in test_trendyol], ensure_ascii=False, indent=2)}

ikas ürünleri:
{json.dumps([{"productId": p["productId"], "name": p["name"]} for p in test_ikas], ensure_ascii=False, indent=2)}

Her Trendyol ürünü için en uygun ikas ürününü eşleştir. JSON döndür.
"""

print("OpenAI test:")
result = _call_openai(prompt)
print(result)

print("\nDeepSeek test:")
result = _call_deepseek(prompt)
print(result)