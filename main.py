import os
import uuid
import json
import msgpack
import asyncio
import traceback
import time
from pathlib import Path
from typing import List, Optional, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect, Query, status
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

import models
import schemas
from database import AsyncSessionLocal, get_db

STORAGE_DIR = Path("storage")
STORAGE_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Advanced Object Storage & Message Broker")

# Povolení CORS pro front-end
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BENCHMARK_MODE = False

# ==========================================
# 1. ČÁST: OBJECT STORAGE & BILLING (Async)
# ==========================================

@app.post("/buckets/", response_model=schemas.BucketResponse, tags=["buckets"])
async def create_bucket(bucket: schemas.BucketCreate, db: AsyncSession = Depends(get_db)):
    db_bucket = models.Bucket(name=bucket.name)
    db.add(db_bucket)
    try:
        await db.commit()
        await db.refresh(db_bucket)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"Skutečná chyba: {str(e)}")
    return db_bucket

@app.get("/buckets/{bucket_id}/objects/", response_model=List[schemas.FileMetadataResponse], tags=["buckets"])
async def list_bucket_objects(bucket_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.Bucket).options(selectinload(models.Bucket.files)).filter(models.Bucket.id == bucket_id)
    )
    bucket = result.scalar_one_or_none()
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket nenalezen")
    
    return [f for f in bucket.files if not f.is_deleted]

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

@app.post("/buckets/{bucket_id}/upload", response_model=schemas.FileMetadataResponse, tags=["files"])
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
    user_path = STORAGE_DIR / bucket_id / user_id
    user_path.mkdir(parents=True, exist_ok=True)
    file_path = user_path / file_id

    content = await file.read()
    file_size = len(content)
    
    with open(file_path, "wb") as f:
        f.write(content)

    bucket.count_write_requests += 1
    bucket.current_storage_bytes += file_size
    
    if x_internal_source:
        bucket.internal_transfer_bytes += file_size
    else:
        bucket.ingress_bytes += file_size

    db_file = models.FileMetadata(
        id=file_id,
        user_id=user_id,
        filename=file.filename,
        path=str(file_path),
        size=file_size,
        bucket_id=bucket_id
    )
    db.add(db_file)
    await db.commit()
    await db.refresh(db_file)
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

    bucket = db_file.bucket
    bucket.count_read_requests += 1
    
    if x_internal_source:
        bucket.internal_transfer_bytes += db_file.size
    else:
        bucket.egress_bytes += db_file.size
    
    await db.commit()
    return FileResponse(path=db_file.path, filename=db_file.filename)

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

    db_file.is_deleted = True
    
    bucket = db_file.bucket
    bucket.current_storage_bytes -= db_file.size
    bucket.count_write_requests += 1
    
    await db.commit()
    return {"message": f"Soubor {db_file.filename} byl úspěšně přesunut do koše (soft-delete)."}


# ==========================================
# 2. ČÁST: MESSAGE BROKER & WORKER ENDPOINTS
# ==========================================

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
        
        if not BENCHMARK_MODE:
            result = await db.execute(
                select(models.QueuedMessage).filter_by(topic=topic, is_delivered=False)
            )
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
        data = {
            "action": "deliver",
            "topic": topic,
            "message_id": msg_id,
            "payload": payload
        }
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
        if not BENCHMARK_MODE:
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
            if is_binary:
                raw_data = await websocket.receive_bytes()
            else:
                raw_data = await websocket.receive_text()
            
            try:
                if is_binary:
                    msg_dict = msgpack.unpackb(raw_data)
                else:
                    msg_dict = json.loads(raw_data)
                
                msg = schemas.BrokerMessage(**msg_dict)
            except Exception as e:
                error_msg = {"action": "error", "message": f"Bad data: {str(e)}"}
                try:
                    lock = manager.locks.get(websocket)
                    if lock:
                        async with lock:
                            if is_binary:
                                await websocket.send_bytes(msgpack.packb(error_msg))
                            else:
                                await websocket.send_json(error_msg)
                except:
                    pass
                continue

            if msg.action == "subscribe":
                current_topic = msg.topic
                if BENCHMARK_MODE:
                    await manager.connect(websocket, current_topic, None, is_binary)
                else:
                    async with AsyncSessionLocal() as db:
                        await manager.connect(websocket, current_topic, db, is_binary)
            
            elif msg.action == "publish":
                if BENCHMARK_MODE:
                    await manager.broadcast(msg.topic, msg.payload, None, source_is_binary=is_binary)
                else:
                    async with AsyncSessionLocal() as db:
                        await manager.broadcast(msg.topic, msg.payload, db, source_is_binary=is_binary)
            
            elif msg.action == "ack":
                if not BENCHMARK_MODE:
                    async with AsyncSessionLocal() as db:
                        await db.execute(
                            update(models.QueuedMessage)
                            .where(models.QueuedMessage.id == msg.message_id)
                            .values(is_delivered=True)
                        )
                        await db.commit()

    except WebSocketDisconnect:
        if current_topic:
            manager.disconnect(websocket, current_topic)
    
    except RuntimeError as e:
        if "websocket.close" in str(e):
            if current_topic:
                manager.disconnect(websocket, current_topic)
        else:
            print(f"Server error (RuntimeError): {e}")
            traceback.print_exc()
            if current_topic:
                manager.disconnect(websocket, current_topic)

    except Exception as e:
        print(f"Server error: {e}")
        traceback.print_exc()
        if current_topic:
            manager.disconnect(websocket, current_topic)

@app.post("/buckets/{bucket_id}/objects/{file_id}/process", status_code=status.HTTP_202_ACCEPTED)
async def process_image_endpoint(
    bucket_id: str, 
    file_id: str, 
    request: schemas.ImageProcessRequest,
    db: AsyncSession = Depends(get_db)
):
    job_id = f"job_{file_id}_{int(time.time())}"
    
    result = await db.execute(
        select(models.FileMetadata).filter(
            models.FileMetadata.id == file_id,
            models.FileMetadata.bucket_id == bucket_id,
            models.FileMetadata.is_deleted == False
        )
    )
    db_file = result.scalar_one_or_none()
    
    if not db_file:
        raise HTTPException(status_code=404, detail="Soubor nebyl nalezen v daném bucketu")
    
    # TADY JE ZMĚNA: Cestu nasměrujeme krásně k původnímu souboru uživatele
    user_id = db_file.user_id
    payload = {
        "job_id": job_id,
        "is_test": False,
        "operation": request.operation,
        "params": request.params,
        "input_path": db_file.path, 
        "output_path": f"./storage/{bucket_id}/{user_id}/processed_{file_id}.jpg" 
    }
    
    await manager.broadcast("image.jobs", payload, db, source_is_binary=False)
    
    return {
        "status": "processing_started", 
        "job_id": job_id,
        "message": "Obrázek byl odeslán ke zpracování na pozadí."
    }

@app.get("/buckets/{bucket_id}/objects/{file_id}/processed", tags=["files"])
async def get_processed_image(bucket_id: str, file_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.FileMetadata).filter(
            models.FileMetadata.id == file_id,
            models.FileMetadata.bucket_id == bucket_id
        )
    )
    db_file = result.scalar_one_or_none()
    
    if not db_file:
        raise HTTPException(status_code=404, detail="Původní soubor nenalezen")

    bucket_dir = STORAGE_DIR / bucket_id
    user_dir = bucket_dir / db_file.user_id

    # 1. Chytré hledání ve složce uživatele (včetně případných zkomolenin jako .jpg.jpg)
    if user_dir.exists():
        for file in user_dir.iterdir():
            if file.is_file() and file_id in file.name and "processed" in file.name:
                return FileResponse(path=str(file), media_type="image/jpeg")
                
    # 2. Záložní chytré hledání přímo v kořeni bucketu
    if bucket_dir.exists():
        for file in bucket_dir.iterdir():
            if file.is_file() and file_id in file.name and "processed" in file.name:
                return FileResponse(path=str(file), media_type="image/jpeg")

    # Pokud ho nenajde ani teď, Worker ho musel uložit úplně jinam
    raise HTTPException(status_code=404, detail="Zpracovaný obrázek nenalezen na disku v žádném formátu.")

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)