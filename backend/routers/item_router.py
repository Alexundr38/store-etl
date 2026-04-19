from typing import List, Union
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from schemas import item_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import item_crud

router = APIRouter(prefix="/item", tags=["item"])


@router.get("/", response_model=List[item_schema.ItemBase])
async def list_items(
        consumer_id: Union[str, UUID],
        category_id: Union[str, UUID],
        page: int = 0,
        db: AsyncSession = Depends(get_db),
    ) -> List[item_schema.ItemBase]:
    try:
        items = await item_crud.get_items_by_category(db, category_id, page)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return items


@router.post("/add", response_model=item_schema.CartItem)
async def add_item_to_cart(
        consumer_id: Union[str, UUID],
        item_id: Union[str, UUID],
        count_items: int = 1,
        db: AsyncSession = Depends(get_db),
    ):
    try:
        cart_item = await item_crud.add_item_to_cart(db, consumer_id, item_id, count_items)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return cart_item