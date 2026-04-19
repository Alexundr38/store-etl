from fastapi import HTTPException, status
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession

from db import models

async def get_categories(db: AsyncSession):
    categories = await db.execute(
        Select(models.Category)
    )
    categories = categories.scalars().all()

    if categories is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return categories