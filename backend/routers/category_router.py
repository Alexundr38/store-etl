from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Union
from schemas import category_schema
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import category_crud

router = APIRouter(prefix="/category", tags=["Category"])

@router.get("/", response_model=List[category_schema.Category])
async def list_categories(
        consumer_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db)
    ) -> List[category_schema.Category]:

    categories = await category_crud.get_categories(db)
    return categories