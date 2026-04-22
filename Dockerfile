FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Python bağımlılıklarını kur
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright chromium kur
RUN playwright install chromium

# Uygulama dosyalarını kopyala
COPY . .

# Port
EXPOSE 8000

# Başlangıç komutu
CMD ["python", "main.py"]