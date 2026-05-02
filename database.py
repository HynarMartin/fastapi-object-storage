from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
import os

def get_url():
    if os.environ.get("TESTING"):
        return "sqlite+aiosqlite:///./test_db.db"
    return "sqlite+aiosqlite:///./sql_app.db"

engine = create_async_engine(
    get_url(),
    connect_args={
        "check_same_thread": False,
        "timeout": 20  
    }
)

AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session