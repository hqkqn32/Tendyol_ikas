#!/usr/bin/env python3
"""
Harici sağlık kontrolü scripti
Cron ile çalıştırılır: */5 * * * * /opt/trendyol_yorumkit/venv/bin/python /opt/trendyol_yorumkit/health_checker.py
"""

import requests
import sys
import os
from telegram_notifier import send_telegram

HEALTH_URL = "http://localhost:8000/health"
TIMEOUT = 10


def check_health():
    try:
        response = requests.get(HEALTH_URL, timeout=TIMEOUT)
        
        if response.status_code != 200:
            error_msg = f"🚨 <b>Sağlık Kontrolü BAŞARISIZ</b>\n\nHTTP {response.status_code}"
            send_telegram(error_msg)
            return False
        
        data = response.json()
        
        # Worker durumu
        if not data.get("worker_running", False):
            error_msg = f"🚨 <b>Worker Çalışmıyor!</b>\n\nServis ayakta ama worker durmuş."
            send_telegram(error_msg)
            return False
        
        # Bellek kontrolü
        mem_percent = data.get("memory_percent", 0)
        if mem_percent > 90:
            warning_msg = f"⚠️ <b>Yüksek Bellek Kullanımı</b>\n\n{mem_percent}% kullanımda"
            send_telegram(warning_msg, silent=True)
        
        # CPU kontrolü
        cpu_percent = data.get("cpu_percent", 0)
        if cpu_percent > 95:
            warning_msg = f"⚠️ <b>Yüksek CPU Kullanımı</b>\n\n{cpu_percent}% kullanımda"
            send_telegram(warning_msg, silent=True)
        
        return True
        
    except requests.exceptions.ConnectionError:
        error_msg = f"🚨 <b>SERVİS ERİŞİLEMİYOR!</b>\n\n{HEALTH_URL} yanıt vermiyor."
        send_telegram(error_msg)
        return False
        
    except Exception as e:
        error_msg = f"🚨 <b>Sağlık Kontrolü Hatası</b>\n\n{str(e)}"
        send_telegram(error_msg)
        return False


if __name__ == "__main__":
    success = check_health()
    sys.exit(0 if success else 1)