import asyncio
import json
import websockets
import numpy as np
from PIL import Image
import traceback

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
            async with websockets.connect(uri) as ws:
                print("Worker připojen k brokeru. Čekám na úlohy...")
                
                # Přihlášení k odběru úloh
                sub_msg = {"action": "subscribe", "topic": "image.jobs"}
                await ws.send(json.dumps(sub_msg))
                
                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)
                    
                    if data.get("action") == "deliver":
                        msg_id = data["message_id"]
                        payload = data["payload"]
                        job_id = payload.get("job_id", "unknown")
                        
                        try:
                            # Zde by proběhlo stažení z S3 Gateway
                            # Pro testovací účely generujeme testovací matici, pokud je to test
                            if payload.get("is_test"):
                                img_array = np.random.randint(0, 256, (200, 200, 3), dtype=np.uint8)
                            else:
                                img = Image.open(payload["input_path"])
                                img_array = np.array(img)
                            
                            # Zpracování přes NumPy
                            result_array = process_image(
                                img_array, 
                                payload.get("operation"), 
                                payload.get("params", {})
                            )
                            
                            # Zde by proběhlo nahrání do S3 Gateway
                            if not payload.get("is_test"):
                                result_img = Image.fromarray(result_array)
                                result_img.save(payload["output_path"], format="JPEG")
                                
                            response_payload = {
                                "status": "success",
                                "job_id": job_id,
                                "operation": payload.get("operation")
                            }
                            print(f"Úloha {job_id} dokončena.")
                            
                        except Exception as e:
                            print(f"Chyba u úlohy {job_id}: {e}")
                            response_payload = {
                                "status": "error",
                                "job_id": job_id,
                                "message": str(e)
                            }
                            
                        # Odeslání zprávy o výsledku
                        done_msg = {
                            "action": "publish",
                            "topic": "image.done",
                            "payload": response_payload
                        }
                        await ws.send(json.dumps(done_msg))
                        
                        # Potvrzení zpracování původní zprávy (ACK)
                        ack_msg = {"action": "ack", "message_id": msg_id}
                        await ws.send(json.dumps(ack_msg))
                        
        except websockets.exceptions.ConnectionClosed:
            print("Spojení s brokerem ztraceno. Obnovuji za 5 sekund...")
            await asyncio.sleep(5)
        except Exception as e:
            traceback.print_exc()
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(image_worker())