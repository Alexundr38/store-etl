from fastapi import APIRouter, Depends, HTTPException, status
from schemas import item_schema, cart_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import cart_crud
from typing import Union, List
from uuid import UUID
from dependencies import get_logger
from services.logger_service import ClickHouseLogger

router = APIRouter(prefix="/cart", tags=["cart"])

@router.get("/", status_code=status.HTTP_200_OK)
async def get_cart(
        consumer_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ) -> List[item_schema.CartItem]:
    try:
        cart_items = await cart_crud.get_cart_items(db, consumer_id)
        for item in cart_items:
            await logger.log_event(
                event_type="get_cart",
                endpoint="/cart/",
                http_method="GET",
                status_code=status.HTTP_200_OK,
                consumer_id=consumer_id,
                item_id=item.item_id,
            )
        return cart_items
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/cart/",
            http_method="GET",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=consumer_id,
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/cart/",
            http_method="GET",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=e,
            consumer_id=consumer_id,
        )
        raise e

@router.post("/create_order/", status_code=status.HTTP_201_CREATED)
async def create_order(
        create_data: cart_schema.CartOrder,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ) -> cart_schema.OrderReturn:
    try:
        order_return = await cart_crud.create_order(db, create_data.consumer_id, create_data.store_id, create_data.items)
        for item in order_return.order_items:
            await logger.log_event(
                event_type="create_order",
                endpoint="/cart/create_order/",
                http_method="POST",
                status_code=status.HTTP_201_CREATED,
                consumer_id=create_data.consumer_id,
                item_id=item.item_id,
                order_id=order_return.order_id,
                store_id=order_return.store_id,
                count_item=item.count_item,
            )
        return order_return
    except HTTPException as e:
        for item in create_data.items:
            await logger.log_event(
                event_type="api_error",
                endpoint="/cart/create_order/",
                http_method="POST",
                status_code=e.status_code,
                error_message=str(e.detail),
                consumer_id=create_data.consumer_id,
                store_id=create_data.store_id,
                item_id=item.item_id
            )
        raise e
    except Exception as e:
        for item in create_data.items:
            await logger.log_event(
                event_type="api_error",
                endpoint="/cart/create_order/",
                http_method="POST",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_message=e,
                consumer_id=create_data.consumer_id,
                store_id=create_data.store_id,
                item_id=item.item_id
            )
        raise e

@router.delete('/delete/', status_code=status.HTTP_204_NO_CONTENT)
async def delete_cart_item(
        consumer_id: Union[UUID, str],
        item_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    try:
        await cart_crud.delete_cart_item(db, consumer_id, item_id)
        await logger.log_event(
            event_type="delete_cart_item",
            endpoint="/cart/delete/",
            http_method="DELETE",
            status_code=status.HTTP_204_NO_CONTENT,
            consumer_id=consumer_id,
            item_id=item_id,
        )
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/cart/delete/",
            http_method="DELETE",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=consumer_id,
            item_id=item_id,
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/cart/delete/",
            http_method="DELETE",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=e,
            consumer_id=consumer_id,
            item_id=item_id,
        )
        raise e