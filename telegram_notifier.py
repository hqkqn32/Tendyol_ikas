import requests
import os
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(message: str, silent: bool = False):
    """
    Telegram'a bildirim gönder
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials eksik, bildirim gönderilemedi")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_notification": silent,
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Telegram gönderim hatası: {e}")
        return False


def notify_error(error_msg: str, job_info: dict = None):
    """
    Hata bildirimi gönder
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    message = f"🚨 <b>Trendyol Scraper Hatası</b>\n\n"
    message += f"⏰ <b>Zaman:</b> {timestamp}\n"
    
    if job_info:
        message += f"🏪 <b>Store:</b> {job_info.get('store_id', 'N/A')}\n"
        message += f"🔢 <b>Seller:</b> {job_info.get('seller_id', 'N/A')}\n"
        message += f"📋 <b>Queue ID:</b> {job_info.get('queue_id', 'N/A')}\n\n"
    
    message += f"❌ <b>Hata:</b>\n<code>{error_msg}</code>"
    
    send_telegram(message)


def notify_success(result: dict, job_info: dict):
    """
    Başarı bildirimi gönder (sadece önemli işler için)
    """
    message = f"✅ <b>Scraping Tamamlandı</b>\n\n"
    message += f"🏪 <b>Store:</b> {job_info.get('store_id', 'N/A')}\n"
    message += f"🔢 <b>Seller:</b> {job_info.get('seller_id', 'N/A')}\n\n"
    message += f"📊 <b>Sonuç:</b>\n"
    message += f"   • Yorum: {result.get('total_saved', 0)} yeni\n"
    message += f"   • Duplicate: {result.get('total_skipped', 0)}\n"
    message += f"   • Ürün: {result.get('unique_products', 0)}\n"
    message += f"   • Süre: {result.get('elapsed', 0)}s"
    
    send_telegram(message, silent=True)


def notify_service_start():
    """
    Servis başlangıç bildirimi
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"🚀 <b>Trendyol Scraper Başlatıldı</b>\n\n⏰ {timestamp}"
    send_telegram(message)


def notify_service_crash(error_msg: str):
    """
    Servis çökmesi bildirimi
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"💥 <b>SERVİS ÇÖKTÜ!</b>\n\n"
    message += f"⏰ <b>Zaman:</b> {timestamp}\n"
    message += f"❌ <b>Hata:</b>\n<code>{error_msg}</code>"
    send_telegram(message)