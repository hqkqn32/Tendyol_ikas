import os
import time
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection(retries=3, delay=5):
    """
    PostgreSQL bağlantısı döner (retry mekanizması ile)
    """
    for i in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.cursor_factory = psycopg2.extras.RealDictCursor
            return conn
        except Exception as e:
            if i < retries - 1:
                print(f"⚠️ DB bağlantı hatası, {delay}s sonra tekrar ({i+1}/{retries}): {e}")
                time.sleep(delay)
            else:
                print(f"❌ DB bağlantısı başarısız oldu: {e}")
                raise