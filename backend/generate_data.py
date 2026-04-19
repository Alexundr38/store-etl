import uuid
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Select
import time
from sqlalchemy import text

from config import get_pg_db
from db.models import Category, Item, Store

DATABASE_URL = get_pg_db()

fake = Faker()

def wait_for_db(engine, max_retries=30, delay=2):
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database is ready")
            return
        except Exception as e:
            print(f"Waiting for database... ({i+1}/{max_retries}) error: {e}")
            time.sleep(delay)
    raise Exception("Database not ready after maximum retries")


def generate_categories(session, count=30):
    categories = []
    for _ in range(count):
        categories.append({
            'category_id': uuid.uuid4(),
            'name': fake.unique.word().capitalize()
        })
    session.bulk_insert_mappings(Category, categories)
    session.commit()
    print(f"Добавлено {count} категорий")
    return categories


def generate_items(session, categories, count=100000):
    items = []
    category_ids = [c['category_id'] for c in categories]
    for _ in range(count):
        items.append({
            'item_id': uuid.uuid4(),
            'category_id': fake.random_element(category_ids),
            'name': fake.word() + " " + fake.word(),
            'price': round(fake.pydecimal(left_digits=3, right_digits=2, positive=True), 2)
        })
    batch_size = 5000
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        session.bulk_insert_mappings(Item, batch)
        session.commit()
        print(f"Добавлено товаров: {i + len(batch)} / {count}")
    print(f"Всего добавлено {count} товаров")


def generate_stores(session, count=500):
    """Генерация магазинов."""
    stores = []
    for _ in range(count):
        stores.append({
            'store_id': uuid.uuid4(),
            'name': f"Магазин {fake.company()}",
            'address': fake.address()
        })
    session.bulk_insert_mappings(Store, stores)
    session.commit()
    print(f"Добавлено {count} магазинов")


if __name__ == "__main__":
    engine = create_engine(DATABASE_URL, echo=False)
    wait_for_db(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        categories = session.execute(
            Select(Category)
        ).all()
        if len(categories) == 0:
            categories = generate_categories(session, count=30)

        items = session.execute(
            Select(Item).limit(50)
        ).all()
        if len(items) == 0:
            generate_items(session, categories, count=500000)

        stores = session.execute(
            Select(Store)
        ).all()
        if len(stores) == 0:
            generate_stores(session, count=500)
    except Exception as e:
        session.rollback()
        print(f"Ошибка: {e}")
    finally:
        session.close()