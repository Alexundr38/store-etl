from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Union
from schemas import category_schema
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import category_crud
from dependencies import get_logger
from services.logger_service import ClickHouseLogger

router = APIRouter(prefix="/category", tags=["Category"])

@router.get("/", response_model=List[category_schema.Category], status_code=status.HTTP_200_OK)
async def list_categories(
        consumer_id: Union[UUID, str],
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger),
    ) -> List[category_schema.Category]:
    try:
        categories = await category_crud.get_categories(db)
        for category in categories:
            await logger.log_event(
                event_type="get_categories",
                endpoint="/category/",
                http_method="GET",
                status_code=status.HTTP_200_OK,
                consumer_id=consumer_id,
                category_id=category.category_id,
            )
        return categories
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/category/",
            http_method="GET",
            status_code=e.status_code,
            error_code=str(e.detail),
            consumer_id=consumer_id
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/category/",
            http_method="GET",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=e,
            consumer_id=consumer_id
        )
        raise e