from uuid import UUID
from typing import Union, List
from pydantic import BaseModel, field_serializer
from datetime import datetime

class DeleteCartItem(BaseModel):
    consumer_id: Union[UUID, str]
    item_id: Union[UUID, str]

class CartOrder(BaseModel):
    consumer_id: Union[UUID, str]
    store_id: Union[UUID, str]
    items: List[Union[UUID, str]]

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None

    @field_serializer('store_id')
    def serialize_store_id(self, store_id: UUID):
        return str(store_id) if store_id else None


class Order(BaseModel):
    order_id: Union[UUID, str]
    consumer_id: Union[UUID, str]
    store_id: Union[UUID, str]
    order_dt: datetime

    @field_serializer('order_id')
    def serialize_order_id(self, order_id: UUID):
        return str(order_id) if order_id else None

    @field_serializer('consumer_id')
    def serialize_consumer_id(self, consumer_id: UUID):
        return str(consumer_id) if consumer_id else None

    @field_serializer('store_id')
    def serialize_store_id(self, store_id: UUID):
        return str(store_id) if store_id else None

class OrderItem(BaseModel):
    order_id: Union[UUID, str]
    item_id: Union[UUID, str]
    count_item: int

    @field_serializer('order_id')
    def serialize_order_id(self, order_id: UUID):
        return str(order_id) if order_id else None

    @field_serializer('item_id')
    def serialize_item_id(self, item_id: UUID):
        return str(item_id) if item_id else None

class OrderReturn(Order):
    order_items: List[OrderItem]