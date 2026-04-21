from uuid import UUID
from typing import Union
from pydantic import BaseModel, field_serializer

class Store(BaseModel):
    store_id: Union[UUID, str]
    name: str
    address: str

    @field_serializer('store_id')
    def serialize_store_id(self, store_id: UUID):
        return str(store_id) if store_id else None