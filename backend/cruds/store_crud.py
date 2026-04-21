from fastapi import HTTPException, status
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession
from db import models

async def get_stores(db: AsyncSession):
    stores = await db.execute(
        Select(models.Store)
    )
    stores = stores.scalars().all()
    if len(stores) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stores not found"
        )
    return stores