import asyncio
import json
import websockets
import numpy as np
from PIL import Image
import traceback
import httpx
import io

def process_image(img_array: np.ndarray, operation: str, params: dict) -> np.ndarray:
    """Provede transformaci obrázku čistě pomocí NumPy matic."""
    
    if operation == "invert":
        # 1. Inverze barev (vektorizované odečtení)
        return 255 - img_array
        
    elif operation == "flip":
        # 2. Horizontální překlopení (slicing: výška zachována, šířka obrácena, barvy zachovány)
        return img_array[:, ::-1, :]
        
    elif operation == "crop":
        # 3. Ořez (Crop) s validací dimenzí
        h, w = img_array.shape[:2]
        top = params.get("top", 0)
        bottom = params.get("bottom", h)
        left = params.get("left", 0)
        right = params.get("right", w)
        
        if top < 0 or bottom > h or left < 0 or right > w or top >= bottom or left >= right:
            raise ValueError(f"Neplatné parametry ořezu. Rozměry obrazu: {w}x{h}.")
            
        return img_array[top:bottom, left:right, :]
        
    elif operation == "brightness":
        # 4. Úprava jasu s prevencí přetečení (saturace)
        offset = params.get("offset", 50)
        # Převod na int16 pro bezpečné přičítání
        temp_array = img_array.astype(np.int16) + offset
        # Oříznutí hodnot a návrat na uint8
        return np.clip(temp_array, 0, 255).astype(np.uint8)
        
    elif operation == "grayscale":
        # 5. Černobílý filtr (Vážený průměr pro lidské oko)
        # Ochrana: pokud má obrázek i alfa kanál (RGBA), vezmeme jen RGB
        rgb = img_array[..., :3]
        # Skalární součin pro rychlý výpočet váženého průměru: 0.299*R + 0.587*G + 0.114*B
        gray = np.dot(rgb, [0.299, 0.587, 0.114])
        return gray.astype(np.uint8)
        
    else:
        raise ValueError(f"Neznámá nebo nepodporovaná operace: {operation}")


async def image_worker():
    uri = "ws://localhost:8000/broker?format=json"
    
    while True:
        try:
            async with websockets.connect(uri, max_size=None) as ws:
                print("🟢 Worker připojen k brokeru. Čekám na úlohy...")
                
                await ws.send(json.dumps({"action": "subscribe", "topic": "image.jobs"}))
                
                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)
                    
                    if data.get("action") == "deliver":
                        msg_id = data["message_id"]
                        payload = data["payload"]
                        job_id = payload.get("job_id", "unknown")
                        
                        try:
                            if payload.get("is_test"):
                                img_array = np.random.randint(0, 256, (200, 200, 3), dtype=np.uint8)
                            else:
                                # 1. STAŽENÍ OBRÁZKU Z S3 GATEWAY DO PAMĚTI
                                file_id = payload["file_id"]
                                bucket_id = payload["bucket_id"]
                                async with httpx.AsyncClient() as client:
                                    resp = await client.get(f"http://127.0.0.1:8000/files/{file_id}")
                                    resp.raise_for_status()
                                    img = Image.open(io.BytesIO(resp.content))
                                    img_array = np.array(img)
                            
                            # 2. ZPRACOVÁNÍ PŘES NUMPY
                            result_array = process_image(
                                img_array, 
                                payload.get("operation"), 
                                payload.get("params", {})
                            )
                            
                            if not payload.get("is_test"):
                                # 3. NAHRÁNÍ VÝSLEDKU ZPĚT DO S3 GATEWAY
                                result_img = Image.fromarray(result_array)
                                buf = io.BytesIO()
                                result_img.save(buf, format="JPEG")
                                buf.seek(0)
                                
                                # Simulujeme odeslání z formuláře, nastavíme název začínající na "processed_"
                                files = {'file': (f"processed_{file_id}.jpg", buf, "image/jpeg")}
                                async with httpx.AsyncClient() as client:
                                    upload_resp = await client.post(
                                        f"http://127.0.0.1:8000/buckets/{bucket_id}/upload?user_id=worker",
                                        files=files,
                                        headers={"X-Internal-Source": "true"},
                                        timeout=20.0  # Zvýšený timeout pro větší obrázky
                                    )
                                    upload_resp.raise_for_status()

                            response_payload = {
                                "status": "success",
                                "job_id": job_id,
                                "operation": payload.get("operation")
                            }
                            print(f"✅ Úloha {job_id} úspěšně dokončena.")
                            
                        except Exception as e:
                            print(f"❌ Chyba u úlohy {job_id}: {e}")
                            traceback.print_exc()
                            response_payload = {
                                "status": "error",
                                "job_id": job_id,
                                "message": str(e)
                            }
                            
                        # 4. ODESLÁNÍ VÝSLEDKU A POTVRZENÍ
                        await ws.send(json.dumps({
                            "action": "publish",
                            "topic": "image.done",
                            "payload": response_payload
                        }))
                        await ws.send(json.dumps({"action": "ack", "message_id": msg_id}))
                        
        except websockets.exceptions.ConnectionClosed:
            print("🔴 Spojení s brokerem ztraceno. Obnovuji za 5 sekund...")
            await asyncio.sleep(5)
        except Exception as e:
            traceback.print_exc()
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(image_worker())