from db import get_connection

try:
    conn = get_connection()
    print("✅ Bağlantı başarılı!")
    conn.close()
except Exception as e:
    print(f"❌ Hata: {e}")