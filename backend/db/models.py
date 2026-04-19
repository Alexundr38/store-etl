import datetime
import uuid

from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy import MetaData, UUID, Column, String, ForeignKey, DECIMAL, Integer, DateTime
from sqlalchemy.ext.asyncio import AsyncAttrs

metadata = MetaData(schema='public')

class Base(AsyncAttrs, DeclarativeBase):
    metadata = metadata


class Category(Base):
    __tablename__ = 'category'

    category_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name = Column(
        String(255),
        nullable=False,
        unique=True
    )

    items = relationship(
        'Item',
        back_populates='category'
    )


class Item(Base):
    __tablename__ = 'item'

    item_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    category_id = Column(
        UUID(as_uuid=True),
        ForeignKey('category.category_id', ondelete='CASCADE'),
        nullable=False
    )
    name = Column(
        String(255),
        nullable=False
    )
    price = Column(
        DECIMAL(8,2),
        nullable=False
    )

    category = relationship(
        'Category',
        back_populates='items'
    )
    cart_items = relationship(
        'CartItem',
        back_populates='item'
    )
    store_items = relationship(
        'StoreItem',
        back_populates='item'
    )
    order_items = relationship(
        'OrderItem',
        back_populates='item'
    )

class Consumer(Base):
    __tablename__ = 'consumer'

    consumer_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name = Column(
        String(255),
        nullable=False
    )
    lastname = Column(
        String(255),
        nullable=False
    )
    patronymic = Column(
        String(255)
    )
    email = Column(
        String(255),
        nullable=False,
        unique=True
    )
    password = Column(
        String(255),
        nullable=False
    )

    carts = relationship(
        'Cart',
        back_populates='consumer'
    )
    orders = relationship(
        'Orders',
        back_populates='consumer'
    )


class Cart(Base):
    __tablename__ = 'cart'

    cart_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    consumer_id = Column(
        UUID(as_uuid=True),
        ForeignKey('consumer.consumer_id', ondelete='CASCADE'),
        nullable=False
    )

    consumer = relationship(
        'Consumer',
        back_populates='carts'
    )
    cart_items = relationship(
        'CartItem',
        back_populates='cart'
    )


class CartItem(Base):
    __tablename__ = 'cart_item'

    cart_id = Column(
        UUID(as_uuid=True),
        ForeignKey('cart.cart_id', ondelete='CASCADE'),
        primary_key=True
    )
    item_id = Column(
        UUID(as_uuid=True),
        ForeignKey('item.item_id', ondelete='CASCADE'),
        primary_key=True
    )
    count_item = Column(
        Integer,
        nullable=False
    )

    cart = relationship(
        'Cart',
        back_populates='cart_items'
    )
    item = relationship(
        'Item',
        back_populates='cart_items'
    )


class Store(Base):
    __tablename__ = 'store'

    store_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name = Column(
        String(255),
        nullable=False
    )
    address = Column(
        String(255),
        nullable=False
    )

    store_items = relationship(
        'StoreItem',
        back_populates='store'
    )
    orders = relationship(
        'Orders',
        back_populates='store'
    )

class StoreItem(Base):
    __tablename__ = 'store_item'

    store_id = Column(
        UUID(as_uuid=True),
        ForeignKey('store.store_id', ondelete='CASCADE'),
        primary_key=True
    )
    item_id = Column(
        UUID(as_uuid=True),
        ForeignKey('item.item_id', ondelete='CASCADE'),
        primary_key=True
    )
    count_item = Column(
        Integer,
        nullable=False
    )

    store = relationship(
        'Store',
        back_populates='store_items'
    )
    item = relationship(
        'Item',
        back_populates='store_items'
    )

class Orders(Base):
    __tablename__ = 'order'

    order_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    consumer_id = Column(
        UUID(as_uuid=True),
        ForeignKey('consumer.consumer_id', ondelete='CASCADE'),
        nullable=False
    )
    store_id = Column(
        UUID(as_uuid=True),
        ForeignKey('store.store_id', ondelete='CASCADE'),
        nullable=False
    )
    order_dt = Column(
        DateTime,
        nullable=False,
        default=datetime.datetime.now()
    )

    consumer = relationship(
        'Consumer',
        back_populates='orders'
    )
    store = relationship(
        'Store',
        back_populates='orders'
    )
    order_items = relationship(
        'OrderItem',
        back_populates='order'
    )


class OrderItem(Base):
    __tablename__ = 'order_item'

    order_id = Column(
        UUID(as_uuid=True),
        ForeignKey('order.order_id', ondelete='CASCADE'),
        primary_key=True
    )
    item_id = Column(
        UUID(as_uuid=True),
        ForeignKey('item.item_id', ondelete='CASCADE'),
        primary_key=True
    )
    count_item = Column(
        Integer,
        nullable=False
    )

    order = relationship(
        'Orders',
        back_populates='order_items'
    )
    item = relationship(
        'Item',
        back_populates='order_items'
    )