from uuid import UUID
from typing import Union, Optional
from pydantic import BaseModel, EmailStr, field_serializer

class ConsumerCreate(BaseModel):
    name: str
    lastname: str
    patronymic: Optional[str] = None
    email: EmailStr
    password: str


class ConsumerUpdate(BaseModel):
    consumer_id: Union[UUID, str]
    name: str
    lastname: str
    patronymic: Optional[str] = None
    email: str

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None


class ConsumerLogin(BaseModel):
    consumer_id: Union[UUID, str]
    password: str

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None


class ConsumerLogout(BaseModel):
    consumer_id: Union[UUID, str]

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None

class ConsumerId(BaseModel):
    consumer_id: Union[UUID, str]

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None