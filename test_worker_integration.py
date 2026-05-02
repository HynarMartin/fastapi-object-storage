import pytest
import asyncio
import numpy as np
from httpx import AsyncClient
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from main import app
from worker import process_image  # Importujeme pouze logiku úprav obrázků

async def mock_in_memory_worker():
    """Tato funkce simuluje chování workera pro in-memory testování."""
    async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
        async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
            await ws.send_json({"action": "subscribe", "topic": "image.jobs"})
            
            while True:
                try:
                    data = await ws.receive_json()
                    if data.get("action") == "deliver":
                        msg_id = data["message_id"]
                        payload = data["payload"]
                        job_id = payload.get("job_id", "unknown")
                        
                        try:
                            # Pro testovací účely generujeme náhodnou matici (fake obrázek)
                            img_array = np.random.randint(0, 256, (200, 200, 3), dtype=np.uint8)
                            
                            # Voláme TVOU logiku z worker.py
                            process_image(img_array, payload.get("operation"), payload.get("params", {}))
                            
                            response_payload = {
                                "status": "success",
                                "job_id": job_id,
                                "operation": payload.get("operation")
                            }
                        except Exception as e:
                            response_payload = {
                                "status": "error",
                                "job_id": job_id,
                                "message": str(e)
                            }
                            
                        # Odeslání výsledku a ACK
                        await ws.send_json({"action": "publish", "topic": "image.done", "payload": response_payload})
                        await ws.send_json({"action": "ack", "message_id": msg_id})
                except Exception:
                    break

@pytest.mark.asyncio
async def test_worker_processing_10_jobs():
    """Testuje zpracování 10 úloh Workerem přes Message Broker (v paměti)."""
    
    received_results = []
    
    # Spuštění in-memory workera jako úkolu na pozadí
    worker_task = asyncio.create_task(mock_in_memory_worker())
    await asyncio.sleep(0.5) # Necháme ho nastartovat a přihlásit se k odběru

    async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
        async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
            
            # Klient naslouchá na dokončené úlohy
            await ws.send_json({"action": "subscribe", "topic": "image.done"})
            await asyncio.sleep(0.1)
            
            # Odeslání 10 úloh do image.jobs
            operations = ["invert", "flip", "grayscale", "brightness", "crop"]
            for i in range(10):
                op = operations[i % len(operations)]
                
                # 9. úloha bude mít schválně neplatný ořez pro otestování chyby
                params = {}
                if op == "crop" and i == 9:
                    params = {"top": -10} 
                elif op == "crop":
                    params = {"top": 10, "bottom": 100, "left": 10, "right": 100}

                payload = {
                    "job_id": f"job_{i}",
                    "is_test": True, 
                    "operation": op,
                    "params": params
                }
                
                await ws.send_json({
                    "action": "publish",
                    "topic": "image.jobs",
                    "payload": payload
                })
            
            # Přijímání 10 odpovědí
            try:
                async with asyncio.timeout(3.0):
                    while len(received_results) < 10:
                        data = await ws.receive_json()
                        if data.get("action") == "deliver":
                            received_results.append(data["payload"])
                            await ws.send_json({"action": "ack", "message_id": data["message_id"]})
            except TimeoutError:
                pass

    worker_task.cancel()
    
    # Ověření výsledků
    assert len(received_results) == 10
    
    success_jobs = [res for res in received_results if res["status"] == "success"]
    assert len(success_jobs) == 9
    
    error_jobs = [res for res in received_results if res["status"] == "error"]
    assert len(error_jobs) == 1
    assert "job_9" == error_jobs[0]["job_id"]
    assert "Neplatné parametry ořezu" in error_jobs[0]["message"]
    
    print("\n✅ Integrace Workera s Brokerem úspěšně prošla.")