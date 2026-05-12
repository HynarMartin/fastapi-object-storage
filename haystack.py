import asyncio
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response
from contextlib import asynccontextmanager
import websockets
import msgpack
import traceback

VOLUME_DIR = Path("volumes")
VOLUME_DIR.mkdir(exist_ok=True)

# Pro lepší vizualizaci fragmentace a dělení záměrně zmenšeno jen na 1 MB!
MAX_VOLUME_SIZE = 1 * 1024 * 1024 

current_volume_id = 1

def init_volumes():
    global current_volume_id
    volume_files = list(VOLUME_DIR.glob("volume_*.dat"))
    if volume_files:
        ids = [int(f.stem.split("_")[1]) for f in volume_files]
        current_volume_id = max(ids)
    print(f"💽 Haystack nastartován. Aktuální svazek: volume_{current_volume_id}.dat")

def write_needle(data: bytes) -> list[dict]:
    """Iterativní zápis po kusech, pokud dojde místo ve svazku."""
    global current_volume_id
    segments = []
    remaining_data = data
    
    while len(remaining_data) > 0:
        volume_path = VOLUME_DIR / f"volume_{current_volume_id}.dat"
        current_size = volume_path.stat().st_size if volume_path.exists() else 0
        
        space_left = MAX_VOLUME_SIZE - current_size
        
        # Pokud je plno, rotujeme
        if space_left <= 0:
            current_volume_id += 1
            print(f"⚠️ Svazek plný. Vytvářím nový disk: volume_{current_volume_id}.dat")
            volume_path = VOLUME_DIR / f"volume_{current_volume_id}.dat"
            current_size = 0
            space_left = MAX_VOLUME_SIZE

        # Zapíšeme jen tolik, kolik se tam vejde
        to_write_size = min(len(remaining_data), space_left)
        chunk = remaining_data[:to_write_size]
        
        with open(volume_path, "ab+") as f:
            offset = f.tell()
            f.write(chunk)
            
        segments.append({
            "volume_id": current_volume_id,
            "offset": offset,
            "size": to_write_size
        })
        
        # Ořízneme zbývající data pro případný další svazek
        remaining_data = remaining_data[to_write_size:]
        
    return segments

async def haystack_broker_client():
    uri = "ws://localhost:8000/broker?format=msgpack"
    while True:
        try:
            async with websockets.connect(uri, max_size=None) as ws:
                print("🟢 Haystack Node připojen k brokeru.")
                await ws.send(msgpack.packb({"action": "subscribe", "topic": "storage.write"}))
                
                while True:
                    raw_data = await ws.recv()
                    msg = msgpack.unpackb(raw_data)
                    
                    if msg.get("action") == "deliver":
                        payload = msg["payload"]
                        object_id = payload["object_id"]
                        binary_data = payload["data"]
                        
                        # 1. Zápis (s případným rozsekáním do více svazků)
                        segments = write_needle(binary_data)
                        print(f"✅ Zapsáno: {object_id} -> Rozděleno do {len(segments)} bloků.")
                        
                        # 2. Vrácení pole s pozicemi všech úseků
                        ack_payload = {
                            "object_id": object_id,
                            "segments": segments
                        }
                        
                        await ws.send(msgpack.packb({
                            "action": "publish",
                            "topic": "storage.ack",
                            "payload": ack_payload
                        }))
                        await ws.send(msgpack.packb({"action": "ack", "message_id": msg["message_id"]}))
                        
        except websockets.exceptions.ConnectionClosed:
            await asyncio.sleep(3)
        except Exception as e:
            traceback.print_exc()
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_volumes()
    task = asyncio.create_task(haystack_broker_client())
    yield
    task.cancel()

app = FastAPI(title="Haystack Storage Node", lifespan=lifespan)

@app.get("/volume/{volume_id}/{offset}/{size}")
async def read_needle(volume_id: int, offset: int, size: int):
    vol_path = VOLUME_DIR / f"volume_{volume_id}.dat"
    if not vol_path.exists():
        raise HTTPException(status_code=404, detail="Svazek neexistuje")
    try:
        with open(vol_path, "rb") as f:
            f.seek(offset)
            data = f.read(size)
        return Response(content=data, media_type="application/octet-stream")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("haystack:app", host="127.0.0.1", port=8001, reload=True)