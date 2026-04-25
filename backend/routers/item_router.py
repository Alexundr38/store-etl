from typing import List, Union
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from schemas import item_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import item_crud
from dependencies import get_logger
from services.logger_service import ClickHouseLogger

router = APIRouter(prefix="/item", tags=["item"])


@router.get("/", response_model=List[item_schema.ItemBase])
async def list_items(
        consumer_id: Union[str, UUID],
        category_id: Union[str, UUID],
        page: int = 0,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ) -> List[item_schema.ItemBase]:
    try:
        items = await item_crud.get_items_by_category(db, category_id, page)
        for item in items:
            await logger.log_event(
                event_type="get_items",
                endpoint="/item/",
                http_method="GET",
                status_code=status.HTTP_200_OK,
                consumer_id=consumer_id,
                item_id=item.item_id,
                category_id=category_id,
            )
        return items
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/item/",
            http_method="GET",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=consumer_id,
            category_id=category_id,
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/item/",
            http_method="GET",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=str(e),
            consumer_id=consumer_id,
            category_id=category_id,
        )
        raise e


@router.post("/add/", response_model=item_schema.CartItem, status_code=status.HTTP_201_CREATED)
async def add_item_to_cart(
        request: item_schema.AddItemRequest,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    try:
        cart_item = await item_crud.add_item_to_cart(db, request.consumer_id, request.item_id, request.count_items)
        await logger.log_event(
            event_type="add_item",
            endpoint="/item/add/",
            http_method="POST",
            status_code=status.HTTP_201_CREATED,
            consumer_id=request.consumer_id,
            item_id=request.item_id,
            count_item=request.count_items,
        )
        return cart_item
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/item/add/",
            http_method="POST",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=request.consumer_id,
            item_id=request.item_id,
            count_items=request.count_items,
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/item/add/",
            http_method="POST",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=str(e),
            consumer_id=request.consumer_id,
            item_id=request.item_id,
            count_items=request.count_items,
        )
        raise e