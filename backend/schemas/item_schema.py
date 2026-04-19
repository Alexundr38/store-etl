from uuid import UUID
from typing import Union
from pydantic import BaseModel, field_serializer


class ItemBase(BaseModel):
    item_id: Union[str, UUID]
    category_id: Union[str, UUID]
    name: str
    price: float

    @field_serializer('item_id')
    def serialize_item_id(self, item_id: UUID):
        return str(item_id) if item_id else None

    @field_serializer('category_id')
    def serialize_category_id(self, category_id: UUID):
        return str(category_id) if category_id else None

class CartItem(BaseModel):
    item_id: Union[str, UUID]
    cart_id: Union[UUID, str]
    count_item: int

    @field_serializer('item_id')
    def serialize_item_id(self, item_id: UUID):
        return str(item_id) if item_id else None

    @field_serializer('cart_id')
    def serialize_cart_id(self, cart_id: UUID):
        return str(cart_id) if cart_id else None