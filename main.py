import os
import uuid
import json
import msgpack
import asyncio
import traceback
import time
from typing import List, Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect, Query, status, Response
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import websockets

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

import models
import schemas
from database import AsyncSessionLocal, get_db

from pathlib import Path

BENCHMARK_MODE = False

# ==========================================
# BACKGROUND TASK: Naslouchání na storage.ack
# ==========================================
async def s3_ack_listener():
    """
    Tento task běží na pozadí a čeká na potvrzení od Haystack Node.
    Jakmile Haystack data fyzicky zapíše, Gateway se to zde dozví a aktualizuje DB.
    """
    uri = "ws://127.0.0.1:8000/broker?format=msgpack"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print("🟢 S3 Gateway Background Task připojen na storage.ack")
                await ws.send(msgpack.packb({"action": "subscribe", "topic": "storage.ack"}))
                
                while True:
                    raw = await ws.recv()
                    msg = msgpack.unpackb(raw)
                    
                    if msg.get("action") == "deliver":
                        payload = msg["payload"]
                        obj_id = payload["object_id"]
                        vol_id = payload["volume_id"]
                        offset = payload["offset"]
                        
                        # Aktualizace v databázi (Eventual Consistency)
                        async with AsyncSessionLocal() as db:
                            await db.execute(
                                update(models.FileMetadata)
                                .where(models.FileMetadata.id == obj_id)
                                .values(status="ready", volume_id=vol_id, offset=offset)
                            )
                            await db.commit()
                            print(f"✅ S3 Gateway: Soubor {obj_id} je nyní 'ready'!")
                            
                        # Potvrzení doručení ACK zprávy brokeru
                        await ws.send(msgpack.packb({"action": "ack", "message_id": msg["message_id"]}))
                        
        except Exception as e:
            await asyncio.sleep(3)

# ==========================================
# INICIALIZACE APLIKACE A BROKERA
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Při startu S3 Gateway se nastartuje i listener na pozadí
    task = asyncio.create_task(s3_ack_listener())
    yield
    # Při vypnutí se bezpečně ukončí
    task.cancel()

app = FastAPI(title="Advanced S3 Gateway & Broker (Haystack Ready)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, dict[WebSocket, bool]] = {}
        self.locks: dict[WebSocket, asyncio.Lock] = {}

    async def connect(self, websocket: WebSocket, topic: str, db: AsyncSession, is_binary: bool):
        if topic not in self.active_connections:
            self.active_connections[topic] = {}
        self.active_connections[topic][websocket] = is_binary
        if websocket not in self.locks:
            self.locks[websocket] = asyncio.Lock()
        
        if not BENCHMARK_MODE and db:
            result = await db.execute(select(models.QueuedMessage).filter_by(topic=topic, is_delivered=False))
            pending = result.scalars().all()
            for msg in pending:
                await self._send_db_msg(websocket, msg, is_binary)

    def disconnect(self, websocket: WebSocket, topic: str):
        if topic in self.active_connections:
            self.active_connections[topic].pop(websocket, None)
            if not self.active_connections[topic]:
                del self.active_connections[topic]
        self.locks.pop(websocket, None)

    async def _send_db_msg(self, websocket: WebSocket, db_msg: models.QueuedMessage, target_is_binary: bool):
        payload = msgpack.unpackb(db_msg.payload) if db_msg.is_binary else json.loads(db_msg.payload)
        await self._send_direct(websocket, db_msg.topic, db_msg.id, payload, target_is_binary)

    async def _send_direct(self, websocket: WebSocket, topic: str, msg_id: int, payload: Any, target_is_binary: bool):
        data = {"action": "deliver", "topic": topic, "message_id": msg_id, "payload": payload}
        lock = self.locks.get(websocket)
        if lock:
            async with lock:
                try:
                    if target_is_binary:
                        await websocket.send_bytes(msgpack.packb(data))
                    else:
                        await websocket.send_json(data)
                except Exception:
                    pass 

    async def broadcast(self, topic: str, payload: Any, db: AsyncSession, source_is_binary: bool = False):
        msg_id = 0
        if not BENCHMARK_MODE and db:
            raw_payload = msgpack.packb(payload) if source_is_binary else json.dumps(payload).encode()
            db_msg = models.QueuedMessage(topic=topic, payload=raw_payload, is_binary=source_is_binary)
            db.add(db_msg)
            await db.commit()
            await db.refresh(db_msg)
            msg_id = db_msg.id

        if topic in self.active_connections:
            connections = list(self.active_connections[topic].items())
            for ws, ws_binary in connections:
                await self._send_direct(ws, topic, msg_id, payload, ws_binary)

manager = ConnectionManager()

@app.websocket("/broker")
async def websocket_endpoint(websocket: WebSocket, format: str = Query("json")):
    await websocket.accept()
    current_topic = None
    is_binary = format == "msgpack"
    try:
        while True:
            raw_data = await websocket.receive_bytes() if is_binary else await websocket.receive_text()
            try:
                msg_dict = msgpack.unpackb(raw_data) if is_binary else json.loads(raw_data)
                msg = schemas.BrokerMessage(**msg_dict)
            except Exception as e:
                continue

            if msg.action == "subscribe":
                current_topic = msg.topic
                async with AsyncSessionLocal() as db:
                    await manager.connect(websocket, current_topic, db, is_binary)
            elif msg.action == "publish":
                async with AsyncSessionLocal() as db:
                    await manager.broadcast(msg.topic, msg.payload, db, source_is_binary=is_binary)
            elif msg.action == "ack":
                async with AsyncSessionLocal() as db:
                    await db.execute(update(models.QueuedMessage).where(models.QueuedMessage.id == msg.message_id).values(is_delivered=True))
                    await db.commit()

    except WebSocketDisconnect:
        if current_topic:
            manager.disconnect(websocket, current_topic)
    except Exception as e:
        if current_topic:
            manager.disconnect(websocket, current_topic)

# ==========================================
# 1. ČÁST: OBJECT STORAGE & BILLING (HAYSTACK)
# ==========================================

@app.post("/buckets/", response_model=schemas.BucketResponse, tags=["buckets"])
async def create_bucket(bucket: schemas.BucketCreate, db: AsyncSession = Depends(get_db)):
    db_bucket = models.Bucket(name=bucket.name)
    db.add(db_bucket)
    await db.commit()
    await db.refresh(db_bucket)
    return db_bucket

@app.get("/buckets/{bucket_id}/billing/", response_model=schemas.BillingResponse, tags=["billing"])
async def get_bucket_billing(bucket_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Bucket).filter(models.Bucket.id == bucket_id))
    bucket = result.scalar_one_or_none()
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket nenalezen")
    return {
        "bucket_name": bucket.name,
        "current_storage_bytes": bucket.current_storage_bytes,
        "ingress_bytes": bucket.ingress_bytes,
        "egress_bytes": bucket.egress_bytes,
        "internal_transfer_bytes": bucket.internal_transfer_bytes,
        "total_api_calls": bucket.count_read_requests + bucket.count_write_requests
    }

@app.post("/buckets/{bucket_id}/upload", response_model=schemas.FileMetadataResponse, status_code=status.HTTP_202_ACCEPTED, tags=["files"])
async def upload_file(
    bucket_id: str,
    user_id: str,
    file: UploadFile = File(...),
    x_internal_source: Optional[bool] = Header(None),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(models.Bucket).filter(models.Bucket.id == bucket_id))
    bucket = result.scalar_one_or_none()
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket nenalezen")

    file_id = str(uuid.uuid4())
    content = await file.read()
    file_size = len(content)

    # Billing účtování
    bucket.count_write_requests += 1
    bucket.current_storage_bytes += file_size
    if x_internal_source:
        bucket.internal_transfer_bytes += file_size
    else:
        bucket.ingress_bytes += file_size

    # 1. ZÁPIS DO DB SE STATUSEM UPLOADING (Eventual consistency)
    db_file = models.FileMetadata(
        id=file_id,
        user_id=user_id,
        filename=file.filename,
        size=file_size,
        bucket_id=bucket_id,
        status="uploading"
    )
    db.add(db_file)
    await db.commit()
    await db.refresh(db_file)

    # 2. ODESLÁNÍ BINÁRNÍCH DAT DO HAYSTACKU PŘES BROKER
    payload = {
        "object_id": file_id,
        "data": content
    }
    await manager.broadcast("storage.write", payload, db, source_is_binary=True)

    return db_file

@app.get("/files/{file_id}", tags=["files"])
async def download_file(
    file_id: str, 
    x_internal_source: Optional[bool] = Header(None),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(models.FileMetadata).options(selectinload(models.FileMetadata.bucket)).filter(
            models.FileMetadata.id == file_id,
            models.FileMetadata.is_deleted == False 
        )
    )
    db_file = result.scalar_one_or_none()
    
    if not db_file:
        raise HTTPException(status_code=404, detail="Soubor nenalezen nebo byl smazán")

    # Pokud se soubor teprve nahrává a ještě nepřišel ACK z Haystacku
    if db_file.status != "ready" or db_file.volume_id is None:
        raise HTTPException(status_code=409, detail="Soubor se ještě nahrává, zkuste to za chvíli.")

    bucket = db_file.bucket
    bucket.count_read_requests += 1
    if x_internal_source:
        bucket.internal_transfer_bytes += db_file.size
    else:
        bucket.egress_bytes += db_file.size
    await db.commit()

    # STAŽENÍ SOUBORU Z HAYSTACK MIKROSLUŽBY (HTTP GET)
    haystack_url = f"http://127.0.0.1:8001/volume/{db_file.volume_id}/{db_file.offset}/{db_file.size}"
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(haystack_url)
            resp.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Chyba při čtení z Haystack Node: {e}")

    # Přeposlání dat zpět uživateli
    return Response(content=resp.content, media_type="image/jpeg")

@app.delete("/files/{file_id}", response_model=schemas.MessageResponse, tags=["files"])
async def delete_file(file_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.FileMetadata).options(selectinload(models.FileMetadata.bucket)).filter(
            models.FileMetadata.id == file_id,
            models.FileMetadata.is_deleted == False
        )
    )
    db_file = result.scalar_one_or_none()
    
    if not db_file:
        raise HTTPException(status_code=404, detail="Soubor nenalezen")

    # SOFT DELETE: Haystack o smazání vůbec neví, jen to skryjeme z databáze
    db_file.is_deleted = True
    
    bucket = db_file.bucket
    bucket.current_storage_bytes -= db_file.size
    bucket.count_write_requests += 1
    
    await db.commit()
    return {"message": f"Soubor {db_file.filename} byl úspěšně přesunut do koše (soft-delete)."}


# 1. Endpoint, který přijme požadavek z Front-endu a pošle job do Brokera
@app.post("/buckets/{bucket_id}/objects/{file_id}/process", status_code=status.HTTP_202_ACCEPTED, tags=["files"])
async def process_image_endpoint(
    bucket_id: str, 
    file_id: str, 
    request: schemas.ImageProcessRequest, 
    db: AsyncSession = Depends(get_db)
):
    # Ověření, že soubor existuje a je připraven
    result = await db.execute(
        select(models.FileMetadata).filter(
            models.FileMetadata.id == file_id,
            models.FileMetadata.bucket_id == bucket_id,
            models.FileMetadata.is_deleted == False
        )
    )
    db_file = result.scalar_one_or_none()
    
    if not db_file or db_file.status != "ready":
        raise HTTPException(status_code=400, detail="Soubor neexistuje nebo se stále nahrává.")

    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "file_id": file_id,
        "bucket_id": bucket_id,
        "operation": request.operation,
        "params": request.params
    }
    
    # Odeslání do fronty
    await manager.broadcast("image.jobs", payload, db, source_is_binary=False)
    
    return {"job_id": job_id, "status": "processing"}


# 2. Endpoint, ze kterého si Frontend stáhne upravený obrázek (volá ho index.html)
@app.get("/buckets/{bucket_id}/objects/{file_id}/processed", tags=["files"])
async def get_processed_image(bucket_id: str, file_id: str, db: AsyncSession = Depends(get_db)):
    # Najdeme soubor, který Worker nahrál s prefixem "processed_"
    result = await db.execute(
        select(models.FileMetadata).filter(
            models.FileMetadata.filename == f"processed_{file_id}.jpg",
            models.FileMetadata.bucket_id == bucket_id,
            models.FileMetadata.is_deleted == False
        ).order_by(models.FileMetadata.created_at.desc())
    )
    db_file = result.scalars().first()
    
    if not db_file or db_file.status != "ready":
        raise HTTPException(status_code=404, detail="Zpracovaný obrázek zatím není k dispozici")

    # Přeposlání dat z Haystack Node (stejná logika jako u běžného downloadu)
    haystack_url = f"http://127.0.0.1:8001/volume/{db_file.volume_id}/{db_file.offset}/{db_file.size}"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(haystack_url)
            resp.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Chyba při čtení z Haystack Node: {e}")

    return Response(content=resp.content, media_type="image/jpeg")


@app.get("/haystack/status", tags=["haystack"])
async def get_haystack_status(db: AsyncSession = Depends(get_db)):
    """Vrací statistiky o Haystack architektuře pro Front-end."""
    # 1. Spočítáme živé a smazané soubory
    result = await db.execute(select(models.FileMetadata))
    all_files = result.scalars().all()
    
    active_files = [f for f in all_files if not f.is_deleted]
    deleted_files = [f for f in all_files if f.is_deleted]
    
    # Mrtvé bajty, které zabírají místo
    wasted_bytes = sum(f.size for f in deleted_files)
    
    # 2. Zjistíme stav fyzických svazků na disku
    VOLUME_DIR = Path("volumes")
    volumes_info = []
    total_physical_size = 0
    
    if VOLUME_DIR.exists():
        for vol_file in VOLUME_DIR.glob("volume_*.dat"):
            size = vol_file.stat().st_size
            total_physical_size += size
            volumes_info.append({
                "name": vol_file.name,
                "size_kb": round(size / 1024, 2)
            })
            
    # Seřadíme svazky podle jména
    volumes_info.sort(key=lambda x: x["name"])

    return {
        "active_count": len(active_files),
        "deleted_count": len(deleted_files),
        "wasted_kb": round(wasted_bytes / 1024, 2),
        "total_physical_kb": round(total_physical_size / 1024, 2),
        "volumes": volumes_info
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)