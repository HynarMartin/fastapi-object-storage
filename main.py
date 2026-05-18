import os, uuid, json, msgpack, asyncio, traceback, time
from typing import List, Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect, Query, status, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn, httpx, websockets

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from pydantic import BaseModel

import models, schemas
from database import AsyncSessionLocal, get_db, engine
from pathlib import Path

BENCHMARK_MODE = False

async def s3_ack_listener():
    uri = "ws://127.0.0.1:8000/broker?format=msgpack"
    while True:
        try:
            async with websockets.connect(uri, max_size=None) as ws:
                print("🟢 S3 Gateway ACK Listener připojen.")
                await ws.send(msgpack.packb({"action": "subscribe", "topic": "storage.ack"}))
                
                while True:
                    raw = await ws.recv()
                    msg = msgpack.unpackb(raw)
                    if msg.get("action") == "deliver":
                        payload = msg["payload"]
                        obj_id = payload["object_id"]
                        segments = payload.get("segments", [])
                        
                        async with AsyncSessionLocal() as db:
                            result = await db.execute(select(models.FileMetadata).where(models.FileMetadata.id == obj_id))
                            db_file = result.scalar_one_or_none()
                            if db_file:
                                db_file.status = "ready"
                                # Uložení všech částí, ze kterých se soubor skládá
                                for idx, seg in enumerate(segments):
                                    db_seg = models.FileSegment(
                                        file_id=obj_id,
                                        segment_index=idx,
                                        volume_id=seg["volume_id"],
                                        offset=seg["offset"],
                                        size=seg["size"]
                                    )
                                    db.add(db_seg)
                                await db.commit()
                                print(f"✅ Gateway: Soubor {obj_id} je složen ze {len(segments)} disků/bloků!")
                        await ws.send(msgpack.packb({"action": "ack", "message_id": msg["message_id"]}))
        except Exception:
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- AUTO-HEAL DATABÁZE ---
    # Při každém startu si S3 Gateway zkontroluje DB a vytvoří chybějící tabulky
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    print("✅ Databáze zkontrolována a tabulky jsou připraveny.")
    
    # Spuštění naslouchání
    task = asyncio.create_task(s3_ack_listener())
    yield
    task.cancel()

app = FastAPI(title="S3 Gateway (Chunking)", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.locks = {}

    async def connect(self, websocket: WebSocket, topic: str, db: AsyncSession, is_binary: bool):
        if topic not in self.active_connections: self.active_connections[topic] = {}
        self.active_connections[topic][websocket] = is_binary
        if websocket not in self.locks: self.locks[websocket] = asyncio.Lock()
        
        if not BENCHMARK_MODE and db:
            result = await db.execute(select(models.QueuedMessage).filter_by(topic=topic, is_delivered=False))
            for msg in result.scalars().all():
                await self._send_db_msg(websocket, msg, is_binary)

    def disconnect(self, websocket: WebSocket, topic: str):
        if topic in self.active_connections:
            self.active_connections[topic].pop(websocket, None)
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
                    await websocket.send_bytes(msgpack.packb(data)) if target_is_binary else await websocket.send_json(data)
                except Exception: pass 

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
            for ws, ws_binary in list(self.active_connections[topic].items()):
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
            except Exception:
                continue

            if msg.action == "subscribe":
                current_topic = msg.topic
                async with AsyncSessionLocal() as db:
                    await manager.connect(websocket, current_topic, db, is_binary)
            elif msg.action == "publish":
                async with AsyncSessionLocal() as db:
                    await manager.broadcast(msg.topic, msg.payload, db, is_binary)
            elif msg.action == "ack":
                async with AsyncSessionLocal() as db:
                    await db.execute(update(models.QueuedMessage).where(models.QueuedMessage.id == msg.message_id).values(is_delivered=True))
                    await db.commit()
    except Exception as e:
        print(f"❌ CHYBA VE WEBSOCKETU: {e}")
        traceback.print_exc()
        if current_topic:
            manager.disconnect(websocket, current_topic)

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
    if not bucket: raise HTTPException(404, "Bucket nenalezen")
    return {"bucket_name": bucket.name, "current_storage_bytes": bucket.current_storage_bytes, "ingress_bytes": bucket.ingress_bytes, "egress_bytes": bucket.egress_bytes, "internal_transfer_bytes": bucket.internal_transfer_bytes, "total_api_calls": bucket.count_read_requests + bucket.count_write_requests}

@app.post("/buckets/{bucket_id}/upload", response_model=schemas.FileMetadataResponse, status_code=status.HTTP_202_ACCEPTED, tags=["files"])
async def upload_file(bucket_id: str, user_id: str, file: UploadFile = File(...), x_internal_source: Optional[bool] = Header(None), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Bucket).filter(models.Bucket.id == bucket_id))
    bucket = result.scalar_one_or_none()
    
    file_id = str(uuid.uuid4())
    content = await file.read()
    file_size = len(content)

    bucket.count_write_requests += 1
    bucket.current_storage_bytes += file_size
    if x_internal_source: bucket.internal_transfer_bytes += file_size
    else: bucket.ingress_bytes += file_size

    db_file = models.FileMetadata(id=file_id, user_id=user_id, filename=file.filename, size=file_size, bucket_id=bucket_id, status="uploading")
    db.add(db_file)
    await db.commit()
    await db.refresh(db_file)

    payload = {"object_id": file_id, "data": content}
    await manager.broadcast("storage.write", payload, db, source_is_binary=True)
    
    # OPRAVA: Ruční vytvoření response modelu, abychom zamezili asynchronnímu lazy-loadingu u prázdných segmentů
    return schemas.FileMetadataResponse(
        id=db_file.id,
        filename=db_file.filename,
        size=db_file.size,
        user_id=db_file.user_id,
        bucket_id=db_file.bucket_id,
        created_at=db_file.created_at,
        is_deleted=db_file.is_deleted,
        status=db_file.status,
        segments=[] # Explicitně prázdné pole, protože segmenty se tvoří asynchronně na pozadí
    )

@app.get("/files/{file_id}", tags=["files"])
async def download_file(file_id: str, x_internal_source: Optional[bool] = Header(None), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.FileMetadata).options(selectinload(models.FileMetadata.bucket), selectinload(models.FileMetadata.segments))
        .filter(models.FileMetadata.id == file_id, models.FileMetadata.is_deleted == False)
    )
    db_file = result.scalar_one_or_none()
    if not db_file: raise HTTPException(404, "Nenalezeno")
    if db_file.status != "ready" or not db_file.segments: raise HTTPException(409, "Soubor se ještě nahrává.")

    bucket = db_file.bucket
    bucket.count_read_requests += 1
    if x_internal_source: bucket.internal_transfer_bytes += db_file.size
    else: bucket.egress_bytes += db_file.size
    await db.commit()

    # MAGIE: Streamování částí z různých Volume souborů
    async def file_iterator():
        sorted_segments = sorted(db_file.segments, key=lambda s: s.segment_index)
        async with httpx.AsyncClient() as client:
            for seg in sorted_segments:
                url = f"http://127.0.0.1:8001/volume/{seg.volume_id}/{seg.offset}/{seg.size}"
                resp = await client.get(url)
                resp.raise_for_status()
                yield resp.content

    return StreamingResponse(file_iterator(), media_type="image/jpeg")

@app.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.FileMetadata).options(selectinload(models.FileMetadata.bucket)).filter(models.FileMetadata.id == file_id, models.FileMetadata.is_deleted == False))
    db_file = result.scalar_one_or_none()
    if not db_file: raise HTTPException(404, "Nenalezeno")
    db_file.is_deleted = True
    bucket = db_file.bucket
    bucket.current_storage_bytes -= db_file.size
    bucket.count_write_requests += 1
    await db.commit()
    return {"message": "Smazáno"}

@app.post("/buckets/{bucket_id}/objects/{file_id}/process", status_code=status.HTTP_202_ACCEPTED, tags=["files"])
async def process_image_endpoint(bucket_id: str, file_id: str, request: schemas.ImageProcessRequest, db: AsyncSession = Depends(get_db)):
    job_id = str(uuid.uuid4())
    payload = {"job_id": job_id, "file_id": file_id, "bucket_id": bucket_id, "operation": request.operation, "params": request.params}
    await manager.broadcast("image.jobs", payload, db, source_is_binary=False)
    return {"job_id": job_id, "status": "processing"}

@app.get("/buckets/{bucket_id}/objects/{file_id}/processed", tags=["files"])
async def get_processed_image(bucket_id: str, file_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(models.FileMetadata).options(selectinload(models.FileMetadata.segments)).filter(
            models.FileMetadata.filename == f"processed_{file_id}.jpg", models.FileMetadata.is_deleted == False
        ).order_by(models.FileMetadata.created_at.desc())
    )
    db_file = result.scalars().first()
    if not db_file or db_file.status != "ready": raise HTTPException(404, "Zatím není k dispozici")

    async def file_iterator():
        sorted_segments = sorted(db_file.segments, key=lambda s: s.segment_index)
        async with httpx.AsyncClient() as client:
            for seg in sorted_segments:
                url = f"http://127.0.0.1:8001/volume/{seg.volume_id}/{seg.offset}/{seg.size}"
                resp = await client.get(url)
                resp.raise_for_status()
                yield resp.content

    return StreamingResponse(file_iterator(), media_type="image/jpeg")

@app.get("/haystack/status", tags=["haystack"])
async def get_haystack_status(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.FileMetadata))
    all_files = result.scalars().all()
    active_files = [f for f in all_files if not f.is_deleted]
    deleted_files = [f for f in all_files if f.is_deleted]
    
    VOLUME_DIR = Path("volumes")
    volumes_info = []
    total_physical_size = 0
    if VOLUME_DIR.exists():
        for vol_file in VOLUME_DIR.glob("volume_*.dat"):
            size = vol_file.stat().st_size
            total_physical_size += size
            volumes_info.append({"name": vol_file.name, "size_kb": round(size / 1024, 2)})
            
    volumes_info.sort(key=lambda x: x["name"])
    return {"active_count": len(active_files), "deleted_count": len(deleted_files), "wasted_kb": round(sum(f.size for f in deleted_files) / 1024, 2), "total_physical_kb": round(total_physical_size / 1024, 2), "volumes": volumes_info}

@app.get("/haystack/volumes/{volume_id}", tags=["haystack"])
async def get_volume_details(volume_id: int, db: AsyncSession = Depends(get_db)):
    """Vrátí detailní rozpis všech segmentů a souborů uvnitř konkrétního svazku."""
    result = await db.execute(
        select(models.FileSegment)
        .options(selectinload(models.FileSegment.file))
        .filter(models.FileSegment.volume_id == volume_id)
        .order_by(models.FileSegment.offset)
    )
    segments = result.scalars().all()

    details = []
    for seg in segments:
        details.append({
            "file_id": seg.file_id,
            "filename": seg.file.filename if seg.file else "Neznámý",
            "is_deleted": seg.file.is_deleted if seg.file else True,
            "offset": seg.offset,
            "size": seg.size,
            "segment_index": seg.segment_index
        })
    return details

# main.py (úplně na konci souboru)
if __name__ == "__main__":
    import uvicorn
    # PŘIDÁNO: ws_max_size=1024*1024*100 (zvedne limit serveru na 100 MB)
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True, ws_max_size=1024*1024*100)