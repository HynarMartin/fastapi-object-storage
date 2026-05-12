import asyncio
import httpx
import time
import io
from PIL import Image

GATEWAY_URL = "http://127.0.0.1:8000"

def create_dummy_image() -> bytes:
    """Vytvoří malý testovací obrázek v paměti."""
    img = Image.new('RGB', (800, 600), color = 'red')
    buf = io.BytesIO()
    img.save(buf, format='JPEG')
    return buf.getvalue()

async def upload_and_process(client: httpx.AsyncClient, bucket_id: str, index: int, operation: str = "grayscale"):
    """Nahraje obrázek a pošle ho ke zpracování s danou operací."""
    
    # 1. Upload obrázku
    img_bytes = create_dummy_image()
    files = {'file': (f"test_{index}.jpg", img_bytes, "image/jpeg")}
    
    resp = await client.post(f"{GATEWAY_URL}/buckets/{bucket_id}/upload?user_id=benchmark", files=files)
    file_data = resp.json()
    file_id = file_data["id"]
    
    # Počkáme na uložení na disk (Eventual consistency)
    while True:
        check_resp = await client.get(f"{GATEWAY_URL}/files/{file_id}")
        if check_resp.status_code == 200:
            break
        await asyncio.sleep(0.5)

    # 2. Odeslání požadavku na zpracování (do fronty)
    start_time = time.time()
    payload = {"operation": operation, "params": {}}
    await client.post(f"{GATEWAY_URL}/buckets/{bucket_id}/objects/{file_id}/process", json=payload)
    
    # 3. Polling na výsledek
    while True:
        res_resp = await client.get(f"{GATEWAY_URL}/buckets/{bucket_id}/objects/{file_id}/processed")
        if res_resp.status_code == 200:
            break
        
        # Timeout pojistka (např. Worker vyhodil chybu a obrázek nikdy nevznikne)
        if time.time() - start_time > 15: 
            if operation == "neznama_operace":
                print(f"❌ Job {index} vypršel (OČEKÁVANÁ CHYBA: Worker správně odmítl neznámou operaci).")
            else:
                print(f"⚠️ Job {index} nečekaně vypršel (timeout).")
            return None
            
        await asyncio.sleep(0.5)
        
    duration = time.time() - start_time
    print(f"✅ Job {index} dokončen za {duration:.2f} s")
    return duration

async def run_benchmark(concurrency: int):
    print(f"🚀 Spouštím Benchmark pro {concurrency} paralelních úloh (včetně jedné chybné)...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Vytvoření testovacího bucketu
        bucket_resp = await client.post(f"{GATEWAY_URL}/buckets/", json={"name": f"bench_{int(time.time())}"})
        bucket_id = bucket_resp.json()["id"]
        
        start_total = time.time()
        
        # Vytvoříme klasické úspěšné úlohy
        tasks = [upload_and_process(client, bucket_id, i, "grayscale") for i in range(concurrency - 1)]
        
        # Přidáme jednu záměrně chybnou úlohu
        error_task_index = concurrency - 1
        tasks.append(upload_and_process(client, bucket_id, error_task_index, "neznama_operace"))
        
        # Spustíme je všechny naráz
        results = await asyncio.gather(*tasks)
        
        total_time = time.time() - start_total
        
        # Filtrace pouze úspěšných časů pro výpočet průměru
        successful_results = [res for res in results if res is not None]
        
        print("-" * 30)
        print("📊 VÝSLEDKY BENCHMARKU")
        print(f"Počet úloh celkem: {concurrency}")
        print(f"Z toho úspěšných: {len(successful_results)}")
        print(f"Z toho selhalo (očekávaně): {concurrency - len(successful_results)}")
        print(f"Celkový běh benchmarku: {total_time:.2f} s")
        
        if successful_results:
            print(f"Průměrný čas jedné úspěšné úlohy: {sum(successful_results)/len(successful_results):.2f} s")

if __name__ == "__main__":
    # Otestujeme 5 úloh (4 projdou, 1 spadne)
    asyncio.run(run_benchmark(concurrency=5))