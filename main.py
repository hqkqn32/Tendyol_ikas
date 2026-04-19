import asyncio
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import get_connection, update_queue_status
from queue_manager import worker
import uvicorn

app = FastAPI(title="Trendyol YorumKit Scraper")



@app.get("/health")
def health():
    return {"status": "ok"}



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)