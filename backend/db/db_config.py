from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeMeta, declarative_base
from typing import AsyncGenerator

from config import get_consumer_db

DATABASE_URL = get_consumer_db()

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    echo=True,
    pool_size=10,
    max_overflow=10,
)

SessionLocal = async_sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base: DeclarativeMeta = declarative_base()

async def get_db() -> AsyncGenerator:
    db = SessionLocal()
    try:
        yield db
    finally:
        await db.close()
