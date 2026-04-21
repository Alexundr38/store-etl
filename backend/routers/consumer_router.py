from fastapi import APIRouter, Depends, status, HTTPException
from schemas import consumer_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import consumer_crud
from dependencies import get_logger
from services.logger_service import ClickHouseLogger
from typing import List

router = APIRouter(prefix="/consumer", tags=["consumer"])

@router.get("/", status_code=status.HTTP_200_OK)
async def get_consumers(
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ) -> List[consumer_schema.ConsumerId]:
    try:
        consumers = await consumer_crud.get_consumers(db)
        for consumer in consumers:
            await logger.log_event(
                event_type="get_consumers",
                endpoint="/consumer/",
                http_method="GET",
                status_code=status.HTTP_200_OK,
                consumer_id=consumer.consumer_id
            )
        return consumers
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/",
            http_method="GET",
            status_code=e.status_code,
            error_message=str(e.detail)
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/",
            http_method="GET",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=e
        )
        raise e

@router.post("/create/", status_code=status.HTTP_201_CREATED)
async def create_consumer(
        consumer: consumer_schema.ConsumerCreate,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    try:
        consumer_id = await consumer_crud.create_consumer(db, consumer)
        await logger.log_event(
            event_type="consumer_created",
            endpoint="/consumer/create/",
            http_method="POST",
            status_code=201,
            consumer_id=consumer_id
        )
        return {"consumer_id": consumer_id}
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/create/",
            http_method="POST",
            status_code=e.status_code,
            error_message=str(e.detail)
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/create/",
            http_method="POST",
            status_code=500,
            error_message=str(e)
        )
        raise e

@router.post("/login/", status_code=status.HTTP_200_OK)
async def login_consumer(
        consumer: consumer_schema.ConsumerLogin,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    try:
        consumer_id = await consumer_crud.login_consumer(db, consumer)
        await logger.log_event(
            event_type="consumer_login",
            endpoint="/consumer/login/",
            http_method="POST",
            status_code=status.HTTP_200_OK,
            consumer_id=consumer_id
        )
        return {"consumer_id": consumer_id}
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/login/",
            http_method="POST",
            status_code=e.status_code,
            error_message=str(e.detail)
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/login/",
            http_method="POST",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=str(e)
        )
        raise e


@router.post('/logout/', status_code=status.HTTP_200_OK)
async def logout_consumer(
        consumer: consumer_schema.ConsumerLogout,
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    await logger.log_event(
        event_type="consumer_logout",
        endpoint="/consumer/logout/",
        http_method="POST",
        status_code=200,
        consumer_id=consumer.consumer_id
    )


@router.post('/update/', status_code=status.HTTP_200_OK)
async def update_consumer(
        consumer: consumer_schema.ConsumerUpdate,
        db: AsyncSession = Depends(get_db),
        logger: ClickHouseLogger = Depends(get_logger)
    ):
    try:
        consumer_id = await consumer_crud.update_consumer(db, consumer)
        await logger.log_event(
            event_type="consumer_update",
            endpoint="/consumer/update/",
            http_method="POST",
            status_code=status.HTTP_200_OK,
            consumer_id=consumer_id,
        )
        return {"consumer_id": consumer_id}
    except HTTPException as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/update/",
            http_method="POST",
            status_code=e.status_code,
            error_message=str(e.detail),
            consumer_id=consumer.consumer_id
        )
        raise e
    except Exception as e:
        await logger.log_event(
            event_type="api_error",
            endpoint="/consumer/update/",
            http_method="POST",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_message=str(e),
            consumer_id=consumer.consumer_id
        )
        raise e
