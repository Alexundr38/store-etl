from fastapi import APIRouter, Depends, HTTPException, status
from schemas import item_schema, cart_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import cart_crud
from typing import Union, List
from uuid import UUID

router = APIRouter(prefix="/cart", tags=["cart"])

@router.get("/", status_code=status.HTTP_200_OK)
async def get_cart(
        consumer_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db)
    ) -> List[item_schema.CartItem]:
    try:
        cart_items = await cart_crud.get_cart_items(db, consumer_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return cart_items

@router.post("/create_order", status_code=status.HTTP_201_CREATED)
async def create_order(
        create_data: cart_schema.CartOrder,
        db: AsyncSession = Depends(get_db)
    ) -> cart_schema.OrderReturn:
    try:
        order_return = await cart_crud.create_order(db, create_data.consumer_id, create_data.store_id, create_data.items)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return order_return

@router.delete('/delete', status_code=status.HTTP_204_NO_CONTENT)
async def delete_cart_item(
        consumer_id: Union[UUID, str],
        item_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db)
    ):
    try:
        await cart_crud.delete_item(db, consumer_id, item_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))