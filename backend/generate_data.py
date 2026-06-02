import uuid
import random
from faker import Faker
from sqlalchemy import create_engine, Select, text
from sqlalchemy.orm import sessionmaker
import time

from config import get_pg_db
from db.models import Category, Item, Store

DATABASE_URL = get_pg_db()
fake = Faker('ru_RU')

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

CATEGORIES_DATA = [
    "Смартфоны", "Ноутбуки", "Планшеты", "Наушники", "Умные часы",
    "Телевизоры", "Фотоаппараты", "Игровые приставки", "Клавиатуры", "Мышки",
    "Холодильники", "Стиральные машины", "Пылесосы", "Микроволновые печи", "Кофеварки",
    "Мужская одежда", "Женская одежда", "Детская одежда", "Обувь", "Сумки",
    "Книги", "Игрушки", "Спорттовары", "Автотовары", "Строительные материалы",
    "Мебель", "Косметика", "Аптека", "Зоотовары", "Канцтовары"
]

def generate_categories(session):
    categories = []
    for name in CATEGORIES_DATA:
        categories.append({
            'category_id': uuid.uuid4(),
            'name': name
        })
    session.bulk_insert_mappings(Category, categories)
    session.commit()
    print(f"Добавлено {len(categories)} категорий")
    return categories

BRANDS = {
    "Смартфоны": ["Apple", "Samsung", "Xiaomi", "Huawei", "Google", "OnePlus", "Realme", "Nokia", "Sony", "LG",
                  "Motorola", "ZTE", "Honor", "Vivo", "Oppo"],
    "Ноутбуки": ["Apple", "Dell", "HP", "Lenovo", "Asus", "Acer", "MSI", "Huawei", "Xiaomi", "Razer", "Gigabyte",
                 "Microsoft", "Samsung", "LG", "Fujitsu"],
    "Планшеты": ["Apple", "Samsung", "Lenovo", "Huawei", "Xiaomi", "Amazon", "Microsoft", "Asus", "Honor", "Realme"],
    "Наушники": ["Sony", "JBL", "Apple", "Samsung", "Xiaomi", "Beats", "Bose", "Sennheiser", "Jabra", "Huawei",
                 "Realme", "OnePlus", "Harman", "AKG", "Panasonic"],
    "Умные часы": ["Apple", "Samsung", "Garmin", "Xiaomi", "Amazfit", "Huawei", "Fitbit", "Suunto", "Polar", "Realme",
                   "OnePlus", "Honor"],
    "Телевизоры": ["Samsung", "LG", "Sony", "TCL", "Xiaomi", "Hisense", "Philips", "Panasonic", "Sharp", "Haier",
                   "Vizio", "JVC"],
    "Фотоаппараты": ["Canon", "Nikon", "Sony", "Fujifilm", "Panasonic", "Olympus", "Leica", "Pentax", "GoPro", "DJI"],
    "Игровые приставки": ["Sony", "Microsoft", "Nintendo", "Valve", "Sega", "Atari", "Nvidia", "Logitech"],
    "Клавиатуры": ["Logitech", "Razer", "Corsair", "HyperX", "SteelSeries", "Ducky", "Keychron", "Apple", "Microsoft",
                   "Redragon", "Cougar", "Cooler Master", "Bloody", "A4Tech"],
    "Мышки": ["Logitech", "Razer", "Corsair", "SteelSeries", "Microsoft", "Xiaomi", "Apple", "HyperX", "Ducky",
              "Cooler Master", "Zowie", "Bloody", "A4Tech", "Genius"],
    "Холодильники": ["LG", "Samsung", "Bosch", "Indesit", "Atlant", "Haier", "Whirlpool", "Electrolux", "Beko", "Sharp",
                     "Panasonic", "Miele"],
    "Стиральные машины": ["LG", "Samsung", "Bosch", "Indesit", "Whirlpool", "Electrolux", "Beko", "Atlant", "Miele",
                          "Zanussi", "Candy", "Haier"],
    "Пылесосы": ["Dyson", "Xiaomi", "Tefal", "Philips", "Samsung", "Roborock", "iRobot", "Karcher", "Bosch", "Miele",
                 "Zelmer", "Thomas"],
    "Микроволновые печи": ["Samsung", "LG", "Panasonic", "Sharp", "Bosch", "Whirlpool", "Electrolux", "Beko", "Haier",
                           "Midea", "Tefal"],
    "Кофеварки": ["De'Longhi", "Philips", "Saeco", "Jura", "Nespresso", "Bosch", "Krups", "Siemens", "Melitta",
                  "Gaggia", "Electrolux", "Tefal"],
    "Мужская одежда": ["Nike", "Adidas", "Puma", "Reebok", "Tommy Hilfiger", "Levi's", "Zara", "H&M", "Calvin Klein",
                       "Armani", "Hugo Boss", "Lacoste", "Columbia", "The North Face"],
    "Женская одежда": ["Nike", "Adidas", "Zara", "H&M", "Mango", "Chanel", "Gucci", "Prada", "Tommy Hilfiger", "Levi's",
                       "Reebok", "Puma", "Calvin Klein"],
    "Детская одежда": ["Chicco", "Mothercare", "Nike", "Adidas", "Disney", "Carter's", "H&M", "Zara Kids", "Gap",
                       "Next", "Reima", "Kerry"],
    "Обувь": ["Nike", "Adidas", "Puma", "Reebok", "Timberland", "ECCO", "Clarks", "New Balance", "Asics", "Skechers",
              "Converse", "Vans", "Salomon", "Merrell"],
    "Сумки": ["Herschel", "Eastpak", "Nike", "Adidas", "Samsonite", "Puma", "Dakine", "Jansport", "The North Face",
              "Deuter", "Osprey"],
    "Книги": ["Эксмо", "АСТ", "Питер", "Манн-Иванов-Фербер", "Альпина", "Бомбора", "РОСМЭН", "Феникс", "Наука", "Дрофа",
              "Просвещение", "Лабиринт"],
    "Игрушки": ["LEGO", "Hasbro", "Mattel", "Fisher-Price", "Playmobil", "Funko", "Bandai", "Ravensburger",
                "Melissa & Doug", "VTech", "Tomy"],
    "Спорттовары": ["Nike", "Adidas", "Puma", "Reebok", "Under Armour", "Decathlon", "Wilson", "Head", "Babolat",
                    "Yonex", "Salomon", "Garmin"],
    "Автотовары": ["Castrol", "Mobil", "Bosch", "Denso", "Michelin", "Continental", "Pirelli", "Goodyear",
                   "Bridgestone", "Valvoline", "Liqui Moly"],
    "Строительные материалы": ["Knauf", "Weber", "Ceresit", "Osram", "Makita", "Bosch", "Stanley", "DeWalt", "Tarkett",
                               "Egger", "Kronospan"],
    "Мебель": ["IKEA", "Leroy Merlin", "Hoff", "Mebelion", "Angstrem", "Mario", "Askona", "Ormatek", "Mr.Doors",
               "Lazurit"],
    "Косметика": ["L'Oreal", "Maybelline", "Nivea", "Garnier", "Estée Lauder", "Dior", "Chanel", "Clinique", "MAC",
                  "Yves Rocher", "Avon", "Oriflame"],
    "Аптека": ["Аспирин", "Парацетамол", "Нурофен", "Феназепам", "Корвалол", "Валидол", "Цитрамон", "Мезим",
               "Энтеросгель", "Лоперамид", "Супрастин", "Лоратадин"],
    "Зоотовары": ["Royal Canin", "Purina", "Pedigree", "Acana", "Hills", "Whiskas", "Kitekat", "Pro Plan", "Grandorf",
                  "Monge", "Brit", "Josera"],
    "Канцтовары": ["Erich Krause", "Bic", "Parker", "Berlingo", "Koh-i-Noor", "Stabilo", "Faber-Castell", "Centropen",
                   "Marvy", "Pentel", "Milan", "Kores"]
}

PRICE_RANGES = {
    "Смартфоны": (8000, 120000), "Ноутбуки": (25000, 250000), "Планшеты": (8000, 90000),
    "Наушники": (800, 40000), "Умные часы": (2500, 60000), "Телевизоры": (12000, 250000),
    "Фотоаппараты": (18000, 180000), "Игровые приставки": (12000, 70000), "Клавиатуры": (400, 18000),
    "Мышки": (250, 12000), "Холодильники": (12000, 90000), "Стиральные машины": (12000, 70000),
    "Пылесосы": (4000, 60000), "Микроволновые печи": (2500, 25000), "Кофеварки": (4000, 120000),
    "Мужская одежда": (400, 18000), "Женская одежда": (400, 22000), "Детская одежда": (250, 6000),
    "Обувь": (800, 15000), "Сумки": (800, 20000), "Книги": (80, 2500), "Игрушки": (150, 12000),
    "Спорттовары": (250, 25000), "Автотовары": (150, 25000), "Строительные материалы": (50, 7000),
    "Мебель": (800, 120000), "Косметика": (150, 15000), "Аптека": (20, 2000), "Зоотовары": (80, 3500),
    "Канцтовары": (10, 2500)
}



def generate_very_diverse_name(category_name, brand):
    models = [str(i) for i in range(100, 1000)] + [f"X{i}" for i in range(1, 20)] + [f"Pro-{i}" for i in range(1, 10)]
    generations = ["I", "II", "III", "IV", "V", "Gen1", "Gen2", "Gen3", "", "", ""]
    sizes = ["S", "M", "L", "XL", "XXL", "32GB", "64GB", "128GB", "256GB", "512GB", "1TB", "2TB", "8GB", "16GB", "32GB"]
    colors = ["Black", "White", "Red", "Blue", "Green", "Yellow", "Gray", "Silver", "Gold", "Rose Gold", "Space Gray",
              "Midnight", fake.color_name().capitalize()]
    addons = ["Pro", "Max", "Ultra", "Lite", "Air", "Plus", "Mini", "SE", "Premium", "Deluxe", "Limited", "Edition",
              "Expert", "Turbo", "Extreme"]

    fake_word_ru = fake.word().capitalize()
    fake_word_en = fake.word().capitalize()
    fake_city = fake.city()
    fake_company = fake.company().split()[0]
    fake_adj = fake.word().capitalize()

    model_num = random.choice(models)
    gen = random.choice(generations)
    size = random.choice(sizes)
    color = random.choice(colors)
    addon = random.choice(addons)

    if category_name in ["Смартфоны", "Ноутбуки", "Планшеты"]:
        templates = [
            f"{brand} {model_num}{gen} {color}",
            f"{brand} {addon} {model_num}",
            f"{brand} {size} {color}",
            f"{brand} {fake_adj} {addon}",
            f"{brand} {model_num} {color} {size}",
            f"{brand} {fake_city} Edition",
        ]
    elif category_name == "Наушники":
        templates = [
            f"{brand} {addon} наушники {model_num} {color}",
            f"{brand} TWS {size} {color}",
            f"{brand} {fake_word_en} in-ear",
            f"{brand} {gen} {fake_adj} наушники",
        ]
    elif category_name == "Книги":
        title = fake.catch_phrase() if random.random() > 0.5 else fake.sentence(nb_words=4).rstrip('.')
        templates = [
            f"{brand} «{title}»",
            f"{title} — {brand}",
            f"{fake.last_name()} {fake.word()} {title[:20]}",
            f"Книга {fake_adj} {title}",
        ]
    elif category_name == "Аптека":
        drug_forms = ["таблетки", "капсулы", "сироп", "мазь", "гель", "раствор", "спрей"]
        form = random.choice(drug_forms)
        dosage = f"{random.choice([50, 100, 200, 300, 500])} мг"
        templates = [
            f"{brand} {dosage} {form} №{random.randint(10, 100)}",
            f"{brand} {addon} {form}",
            f"{brand} {fake_word_ru} {form}",
            f"{brand} {model_num} мг {form}",
        ]
    else:
        templates = [
            f"{brand} {addon} {fake_word_ru} {model_num}",
            f"{brand} {color} {size}",
            f"{brand} {fake_city} {gen}",
            f"{brand} {fake_company} {addon}",
            f"{fake_adj} {brand} {model_num}",
        ]
    return random.choice(templates)


def generate_items(session, categories_dict, count=500000):
    items_to_insert = []
    batch_size = 5000
    total_inserted = 0
    generated_count = 0
    used_names = set()

    existing_names = set()
    existing_rows = session.query(Item.name).all()
    for row in existing_rows:
        existing_names.add(row[0])
    used_names.update(existing_names)
    print(f"Загружено {len(existing_names)} существующих названий.")

    cat_items = list(categories_dict.items())
    num_cats = len(cat_items)
    cat_index = 0

    while generated_count < count:
        cat_name, cat_id = cat_items[cat_index % num_cats]
        cat_index += 1

        brands = BRANDS.get(cat_name, [cat_name])
        brand = random.choice(brands)
        min_price, max_price = PRICE_RANGES.get(cat_name, (100, 10000))

        max_attempts = 100
        for attempt in range(max_attempts):
            name = generate_very_diverse_name(cat_name, brand)
            if name not in used_names:
                used_names.add(name)
                break
            elif attempt == max_attempts - 1:
                name = f"{name} #{uuid.uuid4().hex[:6]}"
                used_names.add(name)
                break

        price = round(random.uniform(min_price, max_price), 2)
        items_to_insert.append({
            'item_id': uuid.uuid4(),
            'category_id': cat_id,
            'name': name,
            'price': price
        })
        generated_count += 1

        if len(items_to_insert) >= batch_size:
            session.bulk_insert_mappings(Item, items_to_insert)
            session.commit()
            total_inserted += len(items_to_insert)
            print(f"Добавлено товаров: {total_inserted} / {count}")
            items_to_insert = []

    if items_to_insert:
        session.bulk_insert_mappings(Item, items_to_insert)
        session.commit()
        total_inserted += len(items_to_insert)
        print(f"Добавлено товаров: {total_inserted} / {count}")

    print(f"Уникальных названий сгенерировано: {len(used_names) - len(existing_names)}")
    print(f"Всего вставлено записей: {total_inserted}")

def generate_stores(session, count=500):
    stores = []
    for _ in range(count):
        name = fake.company()
        if not name.lower().startswith(('магазин', 'ооо', 'ип', 'зао', 'пао')):
            name = f"Магазин «{name}»"
        stores.append({
            'store_id': uuid.uuid4(),
            'name': name,
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
        categories_db = session.execute(Select(Category)).all()
        if not categories_db:
            categories = generate_categories(session)
            categories_dict = {c['name']: c['category_id'] for c in categories}
        else:
            categories_dict = {cat[0].name: cat[0].category_id for cat in categories_db}
            print("Категории уже существуют, пропускаем создание")

        items_exist = session.query(Item).first() is not None
        if not items_exist:
            generate_items(session, categories_dict, count=500000)
        else:
            print("Товары уже существуют, пропускаем генерацию")

        stores_exist = session.query(Store).first() is not None
        if not stores_exist:
            generate_stores(session, count=500)
        else:
            print("Магазины уже существуют, пропускаем генерацию")

    except Exception as e:
        session.rollback()
        print(f"Ошибка: {e}")
    finally:
        session.close()