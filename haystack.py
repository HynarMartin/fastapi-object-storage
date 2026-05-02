import asyncio
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Response
from contextlib import asynccontextmanager
import websockets
import msgpack
import traceback

# === KONFIGURACE ===
VOLUME_DIR = Path("volumes")
VOLUME_DIR.mkdir(exist_ok=True)

# Pro testovací účely dáme limit 100 MB (v reálu by to byly gigabyty)
MAX_VOLUME_SIZE = 100 * 1024 * 1024 

# Globální proměnná pro sledování aktuálního svazku
current_volume_id = 1

def init_volumes():
    """Při startu projde existující svazky a najde ten nejnovější."""
    global current_volume_id
    
    # Najdeme všechny soubory volume_X.dat
    volume_files = list(VOLUME_DIR.glob("volume_*.dat"))
    if volume_files:
        # Extrahujeme z nich ID (čísla) a najdeme to nejvyšší
        ids = [int(f.stem.split("_")[1]) for f in volume_files]
        current_volume_id = max(ids)
    
    print(f"💽 Haystack nastartován. Aktuální svazek: volume_{current_volume_id}.dat")

def write_needle(data: bytes) -> tuple[int, int, int]:
    """
    Logika append-only zápisu. Rotuje svazky, pokud dojde místo.
    Vrací: (volume_id, offset, size)
    """
    global current_volume_id
    payload_size = len(data)
    
    volume_path = VOLUME_DIR / f"volume_{current_volume_id}.dat"
    
    # ROTACE SVAZKU: Pokud by zápis přesáhl limit, vytvoříme nový soubor
    if volume_path.exists() and volume_path.stat().st_size + payload_size > MAX_VOLUME_SIZE:
        print(f"⚠️ Svazek {current_volume_id} je plný. Vytvářím nový...")
        current_volume_id += 1
        volume_path = VOLUME_DIR / f"volume_{current_volume_id}.dat"

    # ZÁPIS NA KONEC SOUBORU (ab+)
    with open(volume_path, "ab+") as f:
        offset = f.tell()  # Zjistíme aktuální pozici (konec souboru)
        f.write(data)      # Zapíšeme binární payload
        
    return current_volume_id, offset, payload_size

async def haystack_broker_client():
    """Klient na pozadí, který komunikuje přes Message Broker."""
    uri = "ws://localhost:8000/broker?format=msgpack"
    
    while True:
        try:
            # Připojujeme se v msgpack módu, protože budeme přijímat surové binární obrázky
            async with websockets.connect(uri) as ws:
                print("🟢 Haystack Node připojen k brokeru. Čekám na data...")
                
                # Přihlášení k odběru dat k zapsání
                await ws.send(msgpack.packb({"action": "subscribe", "topic": "storage.write"}))
                
                while True:
                    raw_data = await ws.recv()
                    msg = msgpack.unpackb(raw_data)
                    
                    if msg.get("action") == "deliver":
                        payload = msg["payload"]
                        object_id = payload["object_id"]
                        binary_data = payload["data"]  # Skutečná fotka v bajtech
                        
                        # 1. Fyzický zápis na disk
                        vol_id, offset, size = write_needle(binary_data)
                        print(f"✅ Zapsáno: {object_id} -> Vol {vol_id}, Offset {offset}, Size {size}")
                        
                        # 2. Sestavení potvrzení (ACK zpráva pro S3 Gateway)
                        ack_payload = {
                            "object_id": object_id,
                            "volume_id": vol_id,
                            "offset": offset,
                            "size": size
                        }
                        
                        # 3. Odeslání potvrzení
                        await ws.send(msgpack.packb({
                            "action": "publish",
                            "topic": "storage.ack",
                            "payload": ack_payload
                        }))
                        
                        # 4. Potvrzení doručení původní zprávy
                        await ws.send(msgpack.packb({
                            "action": "ack", 
                            "message_id": msg["message_id"]
                        }))
                        
        except websockets.exceptions.ConnectionClosed:
            print("🔴 Spojení s brokerem ztraceno. Zkusím znovu za 3 sekundy...")
            await asyncio.sleep(3)
        except Exception as e:
            traceback.print_exc()
            await asyncio.sleep(3)


# === FASTAPI APLIKACE ===

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Spustí se při startu serveru
    init_volumes()
    # Spuštění naslouchání brokerovi na pozadí, aniž by to zablokovalo API
    task = asyncio.create_task(haystack_broker_client())
    yield
    # Spustí se při vypnutí
    task.cancel()

app = FastAPI(title="Haystack Storage Node", lifespan=lifespan)

@app.get("/volume/{volume_id}/{offset}/{size}")
async def read_needle(volume_id: int, offset: int, size: int):
    """
    Tento endpoint dělá jedinou věc: extrémně rychle skočí na offset 
    a vrátí 'size' bajtů (fotku). Žádné složky, žádné hledání v DB.
    """
    vol_path = VOLUME_DIR / f"volume_{volume_id}.dat"
    
    if not vol_path.exists():
        raise HTTPException(status_code=404, detail="Svazek neexistuje")
        
    try:
        # ČTENÍ POMOCÍ SEEK
        with open(vol_path, "rb") as f:
            f.seek(offset)       # Rychlý skok (přeskočení tisíců fotek)
            data = f.read(size)  # Přečtení přesně 1 fotky
            
        return Response(content=data, media_type="image/jpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Haystack poběží na portu 8001, aby nekolidoval s S3 Gateway (8000)
    uvicorn.run("haystack:app", host="127.0.0.1", port=8001, reload=True)