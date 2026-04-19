from uuid import UUID
from pydantic import BaseModel, field_serializer
from typing import Union

class Category(BaseModel):
    category_id: Union[str, UUID]
    name: str

    @field_serializer('category_id')
    def serialize_category_id(self, category_id: UUID) -> str:
        return str(category_id) if category_id else None