# init_db.py
import asyncio
from database import engine, Base
import models

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Databáze byla úspěšně inicializována.")

if __name__ == "__main__":
    asyncio.run(init())