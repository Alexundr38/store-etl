from fastapi import APIRouter, Depends, status
from schemas import consumer_schema
from sqlalchemy.ext.asyncio import AsyncSession
from db.db_config import get_db
from cruds import consumer_crud

router = APIRouter(prefix="/consumer", tags=["consumer"])

@router.post("/create/", status_code=status.HTTP_201_CREATED)
async def create_consumer(
        consumer: consumer_schema.ConsumerCreate,
        db: AsyncSession = Depends(get_db)
    ):
    try:
        consumer_id = await consumer_crud.create_consumer(db, consumer)
    except Exception as e:
        raise e

    return {"consumer_id": consumer_id}

@router.post("/login/", status_code=status.HTTP_200_OK)
async def login_consumer(
        consumer: consumer_schema.ConsumerLogin,
        db: AsyncSession = Depends(get_db)
    ):
    try:
        consumer_id = await consumer_crud.login_consumer(db, consumer)
    except Exception as e:
        raise e
    return {"consumer_id": consumer_id}


@router.post('/logout/', status_code=status.HTTP_200_OK)
async def logout_consumer(
        consumer: consumer_schema.ConsumerLogout
    ):
    print('consumer logout')


@router.post('/update/', status_code=status.HTTP_200_OK)
async def update_consumer(
        consumer: consumer_schema.ConsumerUpdate,
        db: AsyncSession = Depends(get_db)
    ):
    try:
        consumer_id = await consumer_crud.update_consumer(db, consumer)
    except Exception as e:
        raise e
    return {"consumer_id": consumer_id}
