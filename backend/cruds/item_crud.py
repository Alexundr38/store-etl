from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, func, Update
from db import models
from typing import Union
from uuid import UUID


async def get_items_by_category(
        db: AsyncSession,
        category_id: Union[str, UUID],
        page: int = 0,
        page_size: int = 100
    ):
    items_count = await db.execute(
        Select(func.count(models.Item.item_id)).where(models.Item.category_id == category_id)
    )
    items_count = items_count.scalar()

    pagination = int(items_count / page_size)

    offset = pagination * page
    items = await db.execute(
        Select(models.Item)
        .where(models.Item.category_id == category_id)
        .limit(pagination)
        .offset(offset)
    )

    items = items.scalars().all()

    if len(items) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Category {category_id} not found",
        )
    return items


async def add_item_to_cart(
        db: AsyncSession,
        consumer_id: Union[UUID, str],
        item_id: Union[str, UUID],
        count_item: int,
    ):
    consumer = await db.execute(
        Select(models.Consumer).where(models.Consumer.consumer_id == consumer_id)
    )
    consumer = consumer.scalar_one_or_none()
    if consumer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Consumer {consumer_id} not found",
        )

    cart = await db.execute(
        Select(models.Cart).where(models.Cart.consumer_id == consumer_id)
    )
    cart = cart.scalar_one_or_none()
    if cart is None:
        cart = models.Cart(consumer_id=consumer_id)
        db.add(cart)
        await db.flush()
        await db.refresh(cart)

    item = await db.execute(
        Select(models.Item).where(models.Item.item_id == item_id)
    )
    item = item.scalar_one_or_none()
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item {item_id} not found",
        )

    cart_item = await db.execute(
        Select(models.CartItem).where(models.CartItem.item_id == item_id, models.CartItem.cart_id == cart.cart_id)
    )
    cart_item = cart_item.scalar_one_or_none()
    if cart_item is None:
        cart_item = models.CartItem(
            cart_id=cart.cart_id,
            item_id=item_id,
            count_item=count_item,
        )
        db.add(cart_item)
        await db.flush()
        await db.refresh(cart_item)
    else:
        cart_item = await db.execute(
            Update(models.CartItem)
            .where(models.CartItem.cart_id == cart.cart_id, models.CartItem.item_id == item_id)
            .values(count_item=cart_item.count_item + count_item)
            .returning(models.CartItem)
        )
        cart_item = cart_item.scalar_one_or_none()
    await db.commit()
    return cart_item