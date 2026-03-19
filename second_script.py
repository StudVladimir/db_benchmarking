import argparse
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

DATE_START = datetime(2000, 1, 1)
DATE_END = datetime(2026, 12, 31)

FIRSTNAMES = [
    "Alex", "Max", "Ivan", "Pavel", "Dmitry", "Sergey", "Nikita", "Andrey", "Oleg", "Artem",
    "John", "Michael", "David", "James", "Robert", "William", "Daniel", "Thomas", "Kevin", "Mark",
    "Anna", "Maria", "Olga", "Elena", "Irina", "Svetlana", "Natalia", "Ekaterina", "Alina", "Polina",
    "Emily", "Emma", "Olivia", "Sophia", "Mia", "Ava", "Isabella", "Amelia", "Charlotte", "Lily",
]

LASTNAMES = [
    "Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Fedorov", "Morozov", "Novikov",
    "Smith", "Johnson", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas",
    "Klein", "Wagner", "Fischer", "Weber", "Schmidt", "Meyer", "Hoffmann", "Richter", "Bauer", "Keller",
]

# Страны и валидные города по странам (можете расширять)
COUNTRY_CITY = {
    "RU": ["Moscow", "Saint Petersburg", "Kazan", "Novosibirsk", "Yekaterinburg", "Samara", "Ufa", "Perm"],
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Boston", "Miami"],
    "DE": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt", "Stuttgart"],
    "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "ES": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
    "IT": ["Rome", "Milan", "Naples", "Turin", "Bologna"],
    "PL": ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"],
    "CZ": ["Prague", "Brno", "Ostrava"],
    "NL": ["Amsterdam", "Rotterdam", "Utrecht", "The Hague"],
    "SE": ["Stockholm", "Gothenburg", "Malmo"]
}
COUNTRIES = list(COUNTRY_CITY.keys())

STREET_WORDS = ["Main", "Central", "Green", "Oak", "Pine", "Cedar", "Park", "Sunset", "Hill", "River"]
CURRENCIES = ["EUR"]

ORDER_STATUSES = ["created", "paid", "shipped", "cancelled"]
USER_STATUSES = ["active", "blocked", "deleted"]
SELLER_STATUSES = ["active", "blocked"]

# Ровно 15 категорий (уникальные)
CATEGORY_NAMES_15 = [
    "Electronics", "Home", "Sports", "Books", "Toys",
    "Beauty", "Clothing", "Shoes", "Grocery", "Garden",
    "Auto", "Pets", "Office", "Music", "Tools",
]

PRODUCT_ADJ = ["Super", "Mega", "Ultra", "Eco", "Smart", "Pro", "Mini", "Max", "Plus", "Prime"]
PRODUCT_NOUN = ["Phone", "Laptop", "Headphones", "Table", "Chair", "Backpack", "Bottle", "Watch", "Camera", "Keyboard"]

def rand_date(start=DATE_START, end=DATE_END) -> datetime:
    delta = end - start
    sec = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=sec)

def rand_birthdate():
    start = datetime(1950, 1, 1)
    end = datetime(2008, 12, 31)
    return rand_date(start, end).date().isoformat()

def rand_phone():
    return "".join(random.choice(string.digits) for _ in range(11))

def rand_email(first, last, uniq):
    domain = random.choice(["example.com", "mail.test", "demo.local", "bench.io"])
    return f"{first.lower()}.{last.lower()}.{uniq}@{domain}"

def rand_login(first, last, uniq):
    base = f"{first}{last}".lower()
    base = "".join(ch for ch in base if ch.isalnum())
    return f"{base}{uniq}"

def rand_password_hash():
    alphabet = string.ascii_letters + string.digits + "./$"
    return "$2b$12$" + "".join(random.choice(alphabet) for _ in range(53))

def rand_street():
    return f"{random.choice(STREET_WORDS)} {random.randint(1, 200)}"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_csv(path: Path, header: list[str], rows: list[list]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def gen_users(n_users: int):
    header = ["idUser", "Firstname", "Lastname", "Birthdate", "Email", "Phone", "Login", "Password_hash", "Status"]
    rows = []
    for i in range(1, n_users + 1):
        first = random.choice(FIRSTNAMES)
        last = random.choice(LASTNAMES)
        birth = rand_birthdate()
        email = rand_email(first, last, i)
        phone = rand_phone()
        login = rand_login(first, last, i)
        pwd = rand_password_hash()
        status = random.choices(USER_STATUSES, weights=[0.94, 0.05, 0.01], k=1)[0]
        rows.append([i, first, last, birth, email, phone, login, pwd, status])
    return header, rows

def gen_sellers(n_sellers: int):
    header = ["idSeller", "Name", "Rating", "Status", "Register_Date"]
    rows = []
    for i in range(1, n_sellers + 1):
        name = f"Seller_{i:05d}"
        rating = random.randint(0, 5)
        status = random.choices(SELLER_STATUSES, weights=[0.97, 0.03], k=1)[0]
        reg_date = rand_date().date().isoformat()
        rows.append([i, name, rating, status, reg_date])
    return header, rows

def gen_addresses(n_addresses: int):
    header = ["idAddresses", "Country", "City", "Street", "Appartment"]
    rows = []
    for i in range(1, n_addresses + 1):
        country = random.choice(COUNTRIES)
        city = random.choice(COUNTRY_CITY[country])  # <-- город соответствует стране
        street = rand_street()
        app = str(random.randint(1, 155))            # <-- range 1..155
        rows.append([i, country, city, street, app])
    return header, rows

def gen_categories():
    # <-- всегда ровно 15 уникальных категорий, id 1..15
    header = ["idCategory", "Name"]
    rows = [[i + 1, name] for i, name in enumerate(CATEGORY_NAMES_15)]
    return header, rows

def gen_products(n_products: int, n_sellers: int):
    header = ["idProduct", "Title", "Seller_id", "Description", "Price", "Currency", "Created_Date", "Quantity"]
    rows = []
    for i in range(1, n_products + 1):
        title = f"{random.choice(PRODUCT_ADJ)} {random.choice(PRODUCT_NOUN)} {i:05d}"
        seller_id = random.randint(1, n_sellers)
        desc = " ".join(random.choice(PRODUCT_ADJ + PRODUCT_NOUN + STREET_WORDS) for _ in range(20))
        price = f"{random.uniform(1, 999.99):.2f}"
        currency = random.choice(CURRENCIES)
        created = rand_date().strftime("%Y-%m-%d %H:%M:%S")
        qty = random.randint(0, 500)
        rows.append([i, title, seller_id, desc, price, currency, created, qty])
    return header, rows

def gen_orders(n_orders: int, n_users: int, n_addresses: int):
    header = ["idOrder", "Users_id", "Status", "Create_Date", "Paid_Date", "Price_Total", "Currency", "Address_id"]
    rows = []
    for i in range(1, n_orders + 1):
        user_id = random.randint(1, n_users)
        address_id = random.randint(1, n_addresses)

        status = random.choices(ORDER_STATUSES, weights=[0.55, 0.25, 0.15, 0.05], k=1)[0]
        created_dt = rand_date()
        paid_dt = None
        if status in ("paid", "shipped"):
            paid_dt = created_dt + timedelta(minutes=random.randint(1, 60 * 24 * 7))

        price_total = f"{random.uniform(5, 5000):.2f}"
        currency = random.choice(CURRENCIES)

        rows.append([
            i,
            user_id,
            status,
            created_dt.strftime("%Y-%m-%d %H:%M:%S"),
            paid_dt.strftime("%Y-%m-%d %H:%M:%S") if paid_dt else "",
            price_total,
            currency,
            address_id
        ])
    return header, rows

def gen_order_items(n_orders: int, n_products: int, items_per_order_min: int, items_per_order_max: int):
    header = ["Order_id", "Product_id", "Quantity"]
    rows = []
    for order_id in range(1, n_orders + 1):
        k = random.randint(items_per_order_min, items_per_order_max)
        k = min(k, n_products)
        product_ids = random.sample(range(1, n_products + 1), k=k)
        for pid in product_ids:
            qty = random.randint(1, 5)
            rows.append([order_id, pid, qty])
    return header, rows

def gen_product_categories(n_products: int, cats_per_product_min: int, cats_per_product_max: int):
    header = ["Product_id", "Category_id"]
    rows = []
    n_categories = 15  # <-- фиксируем 15
    for product_id in range(1, n_products + 1):
        k = random.randint(cats_per_product_min, cats_per_product_max)
        k = min(k, n_categories)
        category_ids = random.sample(range(1, n_categories + 1), k=k)
        for cid in category_ids:
            rows.append([product_id, cid])
    return header, rows

def gen_user_addresses(n_users: int, n_addresses: int, addrs_per_user_min: int, addrs_per_user_max: int):
    header = ["Users_idUser", "Addresses_idAddresses"]
    rows = []
    for user_id in range(1, n_users + 1):
        k = random.randint(addrs_per_user_min, addrs_per_user_max)
        k = min(k, n_addresses)
        address_ids = random.sample(range(1, n_addresses + 1), k=k)
        for aid in address_ids:
            rows.append([user_id, aid])
    return header, rows

def main():
    p = argparse.ArgumentParser(description="Generate CSV test data for mydb schema.")
    p.add_argument("--out", default="data_csv", help="Output directory for CSV files.")
    p.add_argument("--users", type=int, default=10000)
    p.add_argument("--sellers", type=int, default=10000)
    p.add_argument("--addresses", type=int, default=10000)
    # categories параметр больше не нужен, всегда 15
    p.add_argument("--products", type=int, default=10000)
    p.add_argument("--orders", type=int, default=10000)

    p.add_argument("--items-per-order-min", type=int, default=1)
    p.add_argument("--items-per-order-max", type=int, default=5)

    p.add_argument("--cats-per-product-min", type=int, default=1)
    p.add_argument("--cats-per-product-max", type=int, default=3)

    p.add_argument("--addrs-per-user-min", type=int, default=1)
    p.add_argument("--addrs-per-user-max", type=int, default=2)

    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    random.seed(args.seed)

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    header, rows = gen_users(args.users)
    write_csv(out_dir / "Users.csv", header, rows)

    header, rows = gen_sellers(args.sellers)
    write_csv(out_dir / "Sellers.csv", header, rows)

    header, rows = gen_addresses(args.addresses)
    write_csv(out_dir / "Addresses.csv", header, rows)

    header, rows = gen_categories()
    write_csv(out_dir / "Categories.csv", header, rows)

    header, rows = gen_products(args.products, args.sellers)
    write_csv(out_dir / "Products.csv", header, rows)

    header, rows = gen_orders(args.orders, args.users, args.addresses)
    write_csv(out_dir / "Orders.csv", header, rows)

    header, rows = gen_order_items(args.orders, args.products, args.items_per_order_min, args.items_per_order_max)
    write_csv(out_dir / "Order_Items.csv", header, rows)

    header, rows = gen_product_categories(args.products, args.cats_per_product_min, args.cats_per_product_max)
    write_csv(out_dir / "Product_Categories.csv", header, rows)

    header, rows = gen_user_addresses(args.users, args.addresses, args.addrs_per_user_min, args.addrs_per_user_max)
    write_csv(out_dir / "User_Addresses.csv", header, rows)

    print(f"Done. CSV files created in: {out_dir.resolve()}")
    print("Generated files:")
    for fn in [
        "Users.csv", "Sellers.csv", "Addresses.csv", "Categories.csv",
        "Products.csv", "Orders.csv", "Order_Items.csv",
        "Product_Categories.csv", "User_Addresses.csv"
    ]:
        print(f" - {fn}")

if __name__ == "__main__":
    main()