import asyncio
import time
import websockets
import json
import msgpack

NUM_MESSAGES = 100

async def subscriber(fmt):
    count = 0
    is_binary = fmt == "msgpack"
    async with websockets.connect(f"ws://localhost:8000/broker?format={fmt}") as ws:
        sub = {"action": "subscribe", "topic": "bench"}
        await ws.send(msgpack.packb(sub) if is_binary else json.dumps(sub))
        
        total_expected = NUM_MESSAGES * 5
        while count < total_expected:
            raw = await ws.recv()
            data = msgpack.unpackb(raw) if is_binary else json.loads(raw)
            
            ack = {"action": "ack", "message_id": data["message_id"]}
            await ws.send(msgpack.packb(ack) if is_binary else json.dumps(ack))
            
            count += 1
            # if count % 100 == 0:
            #     print(f"Přijato {count}/{total_expected} zpráv...")

async def publisher(fmt):
    await asyncio.sleep(1)
    
    is_binary = fmt == "msgpack"
    async with websockets.connect(f"ws://localhost:8000/broker?format={fmt}") as ws:
        for i in range(NUM_MESSAGES):
            msg = {"action": "publish", "topic": "bench", "payload": {"n": i}}
            data = msgpack.packb(msg) if is_binary else json.dumps(msg)
            await ws.send(data)

async def run_bench(fmt):
    start = time.time()
    
    subs = [subscriber(fmt) for _ in range(5)]
    pubs = [publisher(fmt) for _ in range(5)]
    
    await asyncio.gather(*subs, *pubs)
    
    end = time.time()
    
    total_messages = NUM_MESSAGES * 5
    
    print(f"Formát {fmt.upper()}: {total_messages} zpráv za {end-start:.2f}s ({total_messages/(end-start):.2f} msg/s)")

if __name__ == "__main__":
    print("Spouštím benchmark...")
    asyncio.run(run_bench("json"))
    asyncio.run(run_bench("msgpack"))