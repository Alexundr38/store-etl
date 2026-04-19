from fastapi import HTTPException, status
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession
from db import models
from typing import Union, List
from uuid import UUID
from schemas import cart_schema

async def get_cart_items(db: AsyncSession, consumer_id: Union[UUID, str]):
    cart = await db.execute(
        Select(models.Cart).where(models.Cart.consumer_id == consumer_id)
    )
    cart = cart.scalar_one_or_none()
    if cart is None:
        cart = models.Cart(consumer_id=consumer_id)
        db.add(cart)
        await db.flush()
        await db.refresh(cart)

    cart_items = await db.execute(
        Select(models.CartItem)
        .where(models.CartItem.cart_id == cart.cart_id)
    )
    cart_items = cart_items.scalars().all()
    return cart_items


async def get_cart_items_by_id(
        db: AsyncSession,
        consumer_id: Union[UUID, str],
        items_id: List[Union[UUID, str]]):
    cart = await db.execute(
        Select(models.Cart).where(models.Cart.consumer_id == consumer_id)
    )
    cart = cart.scalar_one_or_none()
    if cart is None:
        cart = models.Cart(consumer_id=consumer_id)
        db.add(cart)
        await db.flush()
        await db.refresh(cart)

    cart_items = await db.execute(
        Select(models.CartItem)
        .where(models.CartItem.cart_id == cart.cart_id,
               models.CartItem.item_id.in_(items_id))
    )
    cart_items = cart_items.scalars().all()
    return cart_items


async def create_order(
        db: AsyncSession,
        consumer_id: Union[UUID, str],
        store_id: Union[UUID, str],
        items: List[Union[UUID, str]]
    ):
    cart_items = await get_cart_items_by_id(db, consumer_id, items)
    if cart_items is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='zero items in cart')

    order = models.Orders(
        consumer_id=consumer_id,
        store_id=store_id,
    )
    db.add(order)
    await db.flush()
    await db.refresh(order)

    order_items = []
    for item in cart_items:
        order_item = models.OrderItem(
            order_id=order.order_id,
            item_id=item.item_id,
            count_item=item.count_item
        )
        db.add(order_item)
        await db.delete(item)
        await db.flush()
        await db.refresh(order_item)
        order_items.append(order_item)
    await db.commit()

    return cart_schema.OrderReturn(
        order_id=order.order_id,
        consumer_id=order.consumer_id,
        store_id=order.store_id,
        order_dt=order.order_dt,
        order_items=[
            cart_schema.OrderItem(
                order_id=oi.order_id,
                item_id=oi.item_id,
                count_item=oi.count_item
            ) for oi in order_items
        ]
    )

async def delete_cart_item(
        db: AsyncSession,
        consumer_id: Union[UUID, str],
        item_id: Union[UUID, str]
    ):
    cart = await db.execute(
        Select(models.Cart).where(models.Cart.consumer_id == consumer_id)
    )
    cart = cart.scalar_one_or_none()
    if cart is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='cart item not found')
    cart_item = await db.execute(
        Select(models.CartItem)
        .where(models.CartItem.item_id == item_id, models.CartItem.cart_id == cart.cart_id)
    )
    cart_item = cart_item.scalar_one_or_none()
    if cart_item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='cart item not found')
    await db.delete(cart_item)
    await db.commit()
