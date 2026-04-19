from fastapi import HTTPException, status
from sqlalchemy import Select, Update
from sqlalchemy.ext.asyncio import AsyncSession

from db import models
from schemas import consumer_schema
from passlib.context import CryptContext

password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password: str) -> str:
    return password_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return password_context.verify(plain_password, hashed_password)


async def create_consumer(db: AsyncSession, consumer: consumer_schema.ConsumerCreate):
    hashed_password = get_password_hash(consumer.password)
    db_consumer = models.Consumer(
        name=consumer.name,
        lastname=consumer.lastname,
        patronymic=consumer.patronymic,
        email=consumer.email,
        password=hashed_password
    )

    db.add(db_consumer)
    await db.flush()
    await db.refresh(db_consumer)
    await db.commit()
    return db_consumer.consumer_id


async def login_consumer(db: AsyncSession, consumer: consumer_schema.ConsumerLogin):
    db_consumer = await db.execute(
        Select(models.Consumer).where(models.Consumer.consumer_id == consumer.consumer_id)
    )

    db_consumer = db_consumer.scalar_one_or_none()
    if db_consumer is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if not verify_password(consumer.password, db_consumer.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    return db_consumer.consumer_id


async def update_consumer(db: AsyncSession, consumer: consumer_schema.ConsumerUpdate):
    db_consumer = await db.execute(
        Update(models.Consumer).
        where(models.Consumer.consumer_id == consumer.consumer_id).
        values(
            consumer_id=consumer.consumer_id,
            name=consumer.name,
            lastname=consumer.lastname,
            patronymic=consumer.patronymic,
            email=consumer.email
        )
        .returning(models.Consumer)
    )

    db_consumer = db_consumer.scalar_one_or_none()
    if db_consumer is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    await db.commit()
    return db_consumer.consumer_id
