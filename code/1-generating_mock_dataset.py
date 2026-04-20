import csv
import random
from datetime import timedelta, datetime, date
from faker import Faker

# =========================
# Configuration
# =========================
NUM_CUSTOMERS = 1_000_000
NUM_SHIPMENTS = 2_000_000
NUM_PRODUCTS  = 1_000_000
NUM_ORDERS    = 2_001_000

fake = Faker()
Faker.seed(42)
random.seed(42)

TODAY = date.today()

# =========================
# 1. Customers -> CSV
# =========================
def generate_customers_csv(csv_path):
    """
    Customers(
        customer_id, full_name, email, phone_number,
        signup_date, country, state, city, zip_code,
        address_line1, prime_flag, preferred_language,
        marketing_opt_in, account_status, birth_year,
        created_at, updated_at
    )
    """
    headers = [
        "customer_id",
        "full_name",
        "email",
        "phone_number",
        "signup_date",
        "country",
        "state",
        "city",
        "zip_code",
        "address_line1",
        "prime_flag",
        "preferred_language",
        "marketing_opt_in",
        "account_status",
        "birth_year",
        "created_at",
        "updated_at"
    ]

    customer_ids = []

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(NUM_CUSTOMERS):
            customer_id = fake.uuid4()
            customer_ids.append(customer_id)

            signup_date = fake.date_between(start_date="-5y", end_date=TODAY)

            created_at = datetime.combine(signup_date, datetime.min.time())
            updated_at = created_at + timedelta(days=random.randint(0, 365))
            if updated_at.date() > TODAY:
                updated_at = datetime.combine(TODAY, datetime.min.time())

            row = [
                customer_id,
                fake.name(),
                fake.unique.email(),
                fake.phone_number(),
                signup_date.isoformat(),
                fake.country(),
                fake.state(),
                fake.city(),
                fake.postcode(),
                fake.street_address(),
                random.choice([0, 1]),
                random.choice(["en_US", "en_GB", "es_ES", "fr_FR", "de_DE"]),
                random.choice([0, 1]),
                random.choice(["active", "active", "active", "suspended", "fraud_review"]),
                random.randint(1940, 2010),
                created_at.isoformat(),
                updated_at.isoformat()
            ]

            writer.writerow(row)

            if (i + 1) % 100_000 == 0:
                print(f"[Customer] Generated {i+1:,} rows")

    return customer_ids


# =========================
# 2. Shipments -> CSV
# =========================
def generate_shipments_csv(csv_path, customer_ids):
    """
    Shipment(
        shipment_id, order_code, customer_id, carrier,
        warehouse_id, ship_date, estimated_delivery,
        actual_delivery, delivery_type, distance_km,
        package_weight_kg, package_volume_cm3, tracking_number,
        delivery_status, failed_attempts, delivery_delay_flag,
        delay_reason_code, return_flag, created_at, updated_at
    )
    """
    headers = [
        "shipment_id",
        "order_code",
        "customer_id",
        "carrier",
        "warehouse_id",
        "ship_date",
        "estimated_delivery",
        "actual_delivery",
        "delivery_type",
        "distance_km",
        "package_weight_kg",
        "package_volume_cm3",
        "tracking_number",
        "delivery_status",
        "failed_attempts",
        "delivery_delay_flag",
        "delay_reason_code",
        "return_flag",
        "created_at",
        "updated_at"
    ]

    carriers = ["Amazon Logistics", "UPS", "FedEx", "USPS", "DHL"]
    warehouses = ["SEA1", "ONT8", "PHX3", "DFW7", "SFO5", "EWR4", "ORD9"]
    delivery_types = ["standard", "expedited", "same_day"]
    delay_reasons = ["none", "weather", "address_issue", "carrier_capacity", "lost", "customer_not_home"]

    shipment_ids = []

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(NUM_SHIPMENTS):
            customer_id = random.choice(customer_ids)

            ship_date = fake.date_between(start_date="-2y", end_date=TODAY)

            estimated_days = random.randint(1, 5)
            est_delivery = ship_date + timedelta(days=estimated_days)
            if est_delivery > TODAY:
                est_delivery = TODAY

            actual_extra = random.randint(-1, 5)
            actual_delivery = est_delivery + timedelta(days=actual_extra)

            if actual_delivery > TODAY:
                actual_delivery = TODAY
            if actual_delivery < est_delivery:
                actual_delivery = est_delivery

            delay_flag = 1 if actual_delivery > est_delivery else 0
            delay_reason = random.choice(delay_reasons[1:]) if delay_flag else "none"

            failed_attempts = random.choices(
                [0, 1, 2],
                weights=[0.9, 0.08, 0.02],
                k=1
            )[0]

            return_flag = random.choices(
                [0, 1],
                weights=[0.95, 0.05],
                k=1
            )[0]

            created_at = datetime.combine(ship_date, datetime.min.time())
            updated_at = created_at + timedelta(days=random.randint(0, 30))
            if updated_at.date() > TODAY:
                updated_at = datetime.combine(TODAY, datetime.min.time())

            shipment_id = fake.uuid4()
            shipment_ids.append(shipment_id)

            row = [
                shipment_id,
                fake.uuid4(),  # order_code（internal logistic number ）
                customer_id,
                random.choice(carriers),
                random.choice(warehouses),
                ship_date.isoformat(),
                est_delivery.isoformat(),
                actual_delivery.isoformat(),
                random.choice(delivery_types),
                random.randint(3, 3000),
                round(random.uniform(0.1, 30.0), 2),
                random.randint(100, 200_000),
                fake.bothify(text="1Z##########"),
                random.choice(["shipped", "in_transit", "out_for_delivery", "delivered", "delayed"]),
                failed_attempts,
                delay_flag,
                delay_reason,
                return_flag,
                created_at.isoformat(),
                updated_at.isoformat()
            ]

            writer.writerow(row)

            if (i + 1) % 200_000 == 0:
                print(f"[Shipment] Generated {i+1:,} rows")

    return shipment_ids


# =========================
# 3. Product -> CSV
# Product(Product_ID, Product_name, Category_ID, Brand,
#         Price, Discount, Stock, Dimension, Weight,
#         Seller_ID, Compliance_flag, Updated_at)
# =========================
def generate_products_csv(csv_path):
    headers = [
        "Product_ID",
        "Product_name",
        "Category_ID",
        "Brand",
        "Price",
        "Discount",
        "Stock",
        "Dimension",
        "Weight",
        "Seller_ID",
        "Compliance_flag",
        "Updated_at"
    ]

    brand_pool = [
        "Acme", "Globex", "Initech", "Umbrella", "Wonka",
        "Stark", "Wayne", "Hooli", "Soylent", "Aperture"
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(NUM_PRODUCTS):
            product_id = fake.uuid4()

            product_name = f"{fake.word().title()} {random.choice(['Widget', 'Gadget', 'Item', 'Pack', 'Set'])}"

            category_id = random.randint(1, 50)  # assume 50 categories

            brand = random.choice(brand_pool + [fake.company()])

            price = round(random.uniform(5.0, 1500.0), 2)

            # discount percentage 0–50%
            discount = round(random.uniform(0.0, 0.5), 2)

            stock = random.randint(0, 10_000)

            # random measurements：LxWxH cm
            L = random.randint(5, 200)
            W = random.randint(5, 200)
            H = random.randint(2, 150)
            dimension = f"{L}x{W}x{H} cm"

            weight = round(random.uniform(0.1, 80.0), 2)  # kg

            seller_id = fake.uuid4()

            compliance_flag = random.choices([0, 1], weights=[0.03, 0.97], k=1)[0]

            updated_at_date = fake.date_between(start_date="-2y", end_date=TODAY)
            updated_at = datetime.combine(updated_at_date, datetime.min.time())

            row = [
                product_id,
                product_name,
                category_id,
                brand,
                price,
                discount,
                stock,
                dimension,
                weight,
                seller_id,
                compliance_flag,
                updated_at.isoformat()
            ]

            writer.writerow(row)

            if (i + 1) % 100_000 == 0:
                print(f"[Product] Generated {i+1:,} rows")


# =========================
# 4. Order RDBMS -> CSV
# Order_RDBMS(
#   Order_ID, Customer_ID, Order_date,
#   Shipping_address_ID, Payment_ID, Total_amount,
#   Order_status, Shipment_ID, Updated_at
# )
# =========================
def generate_orders_csv(csv_path, customer_ids, shipment_ids):
    headers = [
        "Order_ID",
        "Customer_ID",
        "Order_date",
        "Shipping_address_ID",
        "Payment_ID",
        "Total_amount",
        "Order_status",
        "Shipment_ID",
        "Updated_at"
    ]

    order_statuses = [
        "pending",
        "processing",
        "shipped",
        "delivered",
        "cancelled",
        "returned"
    ]

    num_shipments = len(shipment_ids)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(NUM_ORDERS):
            order_id = fake.uuid4()
            customer_id = random.choice(customer_ids)

            order_date = fake.date_between(start_date="-2y", end_date=TODAY)

            shipping_address_id = fake.uuid4()   # Address [Foreign Key]（虚构）
            payment_id = fake.uuid4()            # Payment [Foreign Key]（虚构）

            total_amount = round(random.uniform(5.0, 3000.0), 2)

            order_status = random.choice(order_statuses)

            # 2,001,000 orders > 2,000,000 shipment
            # First 2,000,000  orders tied with Shipment_ID
            # The rest 1,000 orders unshipped（null）
            if i < num_shipments:
                shipment_id = shipment_ids[i]
            else:
                shipment_id = ""   # Unshipped orders

            updated_at_dt = datetime.combine(order_date, datetime.min.time()) + timedelta(
                days=random.randint(0, 30)
            )
            if updated_at_dt.date() > TODAY:
                updated_at_dt = datetime.combine(TODAY, datetime.min.time())

            row = [
                order_id,
                customer_id,
                order_date.isoformat(),
                shipping_address_id,
                payment_id,
                total_amount,
                order_status,
                shipment_id,
                updated_at_dt.isoformat()
            ]

            writer.writerow(row)

            if (i + 1) % 200_000 == 0:
                print(f"[Order_RDBMS] Generated {i+1:,} rows")


# =========================
# 5. Execution Function
# =========================
def main():
    customers_csv = "customers.csv"
    shipments_csv = "shipments.csv"
    products_csv  = "products.csv"
    orders_csv    = "orders.csv"

    print("Generating Customer dataset (CSV)...")
    customer_ids = generate_customers_csv(customers_csv)

    print("Generating Shipment dataset (CSV)...")
    shipment_ids = generate_shipments_csv(shipments_csv, customer_ids)

    print("Generating Product dataset (CSV)...")
    generate_products_csv(products_csv)

    print("Generating Order_RDBMS dataset (CSV)...")
    generate_orders_csv(orders_csv, customer_ids, shipment_ids)

    print("All CSV datasets generated successfully!")


if __name__ == "__main__":
    main()
