from fastapi import APIRouter, Depends, HTTPException, status
from uuid import UUID
from typing import Union, List
from db.db_config import get_db
from dependencies import get_logger
from services.logger_service import ClickHouseLogger
from sqlalchemy.ext.asyncio import AsyncSession
from cruds import store_crud
from schemas import store_schema

router = APIRouter(prefix="/store", tags=["Store"])

@router.get("/", status_code=status.HTTP_200_OK)
async def get_stores(
    consumer_id: Union[UUID, str],
    db: AsyncSession = Depends(get_db),
    logger: ClickHouseLogger = Depends(get_logger)
    ) -> List[store_schema.Store]:
    try:
        stores = await store_crud.get_stores(db)
        for store in stores:
            await logger.log_event(
                event_type="get_stores",
                endpoint="/store/",
                http_method="GET",
                status_code=status.HTTP_200_OK,
                store_id=store.store_id,
                consumer_id=consumer_id
            )
        return stores
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/store/",
            http_method="GET",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=consumer_id
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/store/",
            http_method="GET",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=e,
            consumer_id=consumer_id
        )
        raise e