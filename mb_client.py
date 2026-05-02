import asyncio
import websockets
import json
import msgpack
import sys

async def client(mode: str, topic: str, fmt: str):
    uri = f"ws://localhost:8000/broker?format={fmt}"
    async with websockets.connect(uri) as ws:
        if mode == "sub":
            sub = {"action": "subscribe", "topic": topic}
            await ws.send(msgpack.packb(sub) if fmt == "msgpack" else json.dumps(sub))
            print(f"Odebírám téma: {topic}")
            
            while True:
                raw = await ws.recv()
                data = msgpack.unpackb(raw) if fmt == "msgpack" else json.loads(raw)
                print(f"Zpráva ID {data['message_id']}: {data['payload']}")
                
                ack = {"action": "ack", "message_id": data["message_id"]}
                await ws.send(msgpack.packb(ack) if fmt == "msgpack" else json.dumps(ack))
        
        elif mode == "pub":
            msg = {"action": "publish", "topic": topic, "payload": {"data": "Ahoj!"}}
            await ws.send(msgpack.packb(msg) if fmt == "msgpack" else json.dumps(msg))
            print(f"Publikováno do {topic}")

if __name__ == "__main__":
    m, t, f = sys.argv[1], sys.argv[2], sys.argv[3]
    asyncio.run(client(m, t, f))