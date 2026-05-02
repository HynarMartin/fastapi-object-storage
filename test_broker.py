import pytest
import os
import asyncio
import pytest_asyncio
import msgpack
from httpx import AsyncClient
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport 

os.environ["TESTING"] = "1"

from main import app, manager
from database import Base, get_db
import models  
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

TEST_DB_URL = "sqlite+aiosqlite:///./test_db.db"
test_engine = create_async_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False},
    poolclass=NullPool 
)
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

async def override_get_db():
    async with TestSessionLocal() as session:
        yield session

app.dependency_overrides[get_db] = override_get_db

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    """Vytvoří čistou databázi před každým testem."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield

@pytest.mark.asyncio
async def test_json_flow():
    received = []

    async def run_sub():
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
                await ws.send_json({"action": "subscribe", "topic": "test"})
                data = await ws.receive_json()
                received.append(data)

    async def run_pub():
        await asyncio.sleep(0.2)
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
                await ws.send_json({
                    "action": "publish",
                    "topic": "test",
                    "payload": {"ok": True}
                })

    await asyncio.gather(run_sub(), run_pub())
    
    assert len(received) > 0
    assert received[0]["payload"] == {"ok": True}
    print("\n✅ JSON Flow Passed")

@pytest.mark.asyncio
async def test_msgpack_flow():
    received = []

    async def run_sub():
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=msgpack", ac) as ws:
                await ws.send_bytes(msgpack.packb({"action": "subscribe", "topic": "msg"}))
                raw = await ws.receive_bytes()
                received.append(msgpack.unpackb(raw))

    async def run_pub():
        await asyncio.sleep(0.2)
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=msgpack", ac) as ws:
                await ws.send_bytes(msgpack.packb({
                    "action": "publish",
                    "topic": "msg",
                    "payload": {"val": 1}
                }))

    await asyncio.gather(run_sub(), run_pub())
    
    assert len(received) > 0
    assert received[0]["payload"] == {"val": 1}
    print("\n✅ MessagePack Flow Passed")

@pytest.mark.asyncio
async def test_topic_isolation():
    received_x = []
    
    async def run_sub_x():
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
                await ws.send_json({"action": "subscribe", "topic": "X"})
                
                try:
                    async with asyncio.timeout(1.0):
                        data = await ws.receive_json()
                        received_x.append(data)
                except TimeoutError:
                    pass 

    async def run_pub_y():
        await asyncio.sleep(0.2) 
        async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
            async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
                await ws.send_json({
                    "action": "publish",
                    "topic": "Y",  
                    "payload": {"secret": "data"}
                })

    await asyncio.gather(run_sub_x(), run_pub_y())
    
    assert len(received_x) == 0
    print("\n✅ Topic Isolation Passed")

@pytest.mark.asyncio
async def test_connect_and_disconnect():
    """Test ověřuje, že se klient může připojit a bezpečně odpojit bez pádu serveru."""
    
    async with AsyncClient(transport=ASGIWebSocketTransport(app=app), base_url="http://test") as ac:
        async with aconnect_ws("ws://test/broker?format=json", ac) as ws:
            await ws.send_json({"action": "subscribe", "topic": "test_disconnect"})
            
            await asyncio.sleep(0.1)
            
    assert True
    print("\n✅ Connect and Disconnect Passed")