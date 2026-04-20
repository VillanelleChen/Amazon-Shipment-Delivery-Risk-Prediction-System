import boto3
import pymysql
from datetime import datetime
from collections import defaultdict
import time

# ====================================
# CONFIG
# ====================================
OLTP_DB = "amazon"
DW_SCHEMA = "dw"

MYSQL_HOST = "amazonsql.cepao6y6sh9u.us-east-1.rds.amazonaws.com"
MYSQL_USER = "admin"
MYSQL_PASSWORD = "amazondb"
MYSQL_PORT = 3306

AWS_REGION = "us-east-1"
SESSION_TABLE = "Session"
REVIEWS_TABLE = "reviews"


# ====================================
# MYSQL CONNECTION
# ====================================
def mysql_conn(database=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        port=MYSQL_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

AWS_ACCESS_KEY_ID     = "ASIA5FTZDBPYUYSYAEB6"
AWS_SECRET_ACCESS_KEY = "eSsZF+oopH4LI7vK8kcqaS+GeboP5LuUNar9Nnlp"
AWS_SESSION_TOKEN      = "IQoJb3JpZ2luX2VjEJb//////////wEaCXVzLXdlc3QtMiJIMEYCIQD2msGQoVj/py5GVA1Y+etE5e6AlWxoPZwRVEIpQb7l2QIhAJfGYka0/eYK7I06LUPNl8Mgnyw1R+J5YRMDGii9R0IqKqsCCF8QABoMOTA1NDE4MzEyNjg5IgxxFU2nS8A4MXLg8PYqiAJ5UdXfj3nMAmKPHeGfloVLfrGhTFCyO4hm70k+1mwdZTvK20TQ/EnAdZrdNEnILhEP0EQaKzbt6uBuT5CgUHbz6InTYFk4wfo+zTmNQ6F7cE8aAaWnIbReNpVuz1epVJE5neyfsNV8dUnxa2uECirRQSfIgl7BNsdFIZ5unsk66V2WrpjJgwZLzvaNW6D8rZHxVXsMt9CiVFsg66vqnp+MrOS85tUp/V7nHR6cMZSrApuQOdyBPt10gEw5YGhqQTRHUEUlMrh7v4h8lmfkLb7G3gVDhdXYqWDH2QYsjZFZIk0oAqNknVtTyeGb+bRKeWtaQltKPpbmabr4eqrRbEJLK31Tbs33Zqowos3LyQY6nAFdsDkI3nn0uERvBUMtSMqoWc3h7XgBSnwsHwWkNL0coDiBlkGZtk2i5RAQIYBiyOUDoikwCJTISEIFxTxCwkPimVu93dkG/th0aZ8Yu/J0bGdrGfiqN2J4eTV2plC4dbVkY26JD/pVgfLXUmMws6VdFmRai+sc6jnrNNFxilg5gwuNaCm84Nl1vIn4RHsrBCjQCIp6PtE3GQW00vo="   # only if Emory uses temporary tokens
AWS_REGION = "us-east-1"

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

# ====================================
# TEST DYNAMODB CONNECTION
# ====================================

def test_dynamodb_connection(dynamodb, required_tables):
    print("\n[TEST] Testing DynamoDB connection...")

    # Check list_tables works
    try:
        existing_tables = dynamodb.tables.all()
        table_names = [t.name for t in existing_tables]
        print("[TEST] DynamoDB reachable.")
    except Exception as e:
        print("[ERROR] Cannot connect to DynamoDB.")
        print(e)
        raise SystemExit("Stopping ETL due to DynamoDB connection failure.")

    # Check required tables exist
    for tbl in required_tables:
        if tbl not in table_names:
            print(f"[ERROR] Table not found: {tbl}")
            raise SystemExit("Stopping ETL due to missing DynamoDB tables.")
        else:
            print(f"[TEST] Found table: {tbl}")

    # Try scanning 1 item
    for tbl in required_tables:
        try:
            table = dynamodb.Table(tbl)
            resp = table.scan(Limit=1)
            count = resp.get("Count", 0)
            print(f"[TEST] Table {tbl}: scan OK, sample count={count}")
        except Exception as e:
            print(f"[ERROR] Cannot scan table: {tbl}")
            print(e)
            raise SystemExit("Stopping ETL due to scan failure.")

    print("[TEST] DynamoDB validation complete.\n")


# Call the test
required_tables = ["Session", "reviews"]
test_dynamodb_connection(dynamodb, required_tables)

# ====================================
# CREATE DW SCHEMA
# ====================================
DW_SQL = """
CREATE DATABASE IF NOT EXISTS dw;
USE dw;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_session;
DROP TABLE IF EXISTS dim_review;
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS fact_customer_activity;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(64) PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    signup_date DATE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    prime_flag TINYINT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(255),
    brand VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    week_of_year INT,
    weekday VARCHAR(20),
    is_weekend TINYINT
);

CREATE TABLE IF NOT EXISTS dim_session (
    session_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    session_start DATETIME,
    session_end DATETIME,
    event_count INT,
    view_count INT,
    cart_count INT,
    purchase_count INT,
    total_value DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS dim_review (
    review_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    product_id VARCHAR(64),
    rating INT,
    review_title TEXT,
    review_text TEXT,
    review_timestamp DATETIME,
    verified_purchase TINYINT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    product_id VARCHAR(64),
    order_date DATE,
    order_amount DECIMAL(12,2),
    shipment_id VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS fact_customer_activity (
    activity_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(64),
    date_id DATE,
    session_count INT,
    review_count INT,
    view_count INT,
    cart_count INT,
    purchase_count INT,
    total_session_value DECIMAL(12,2),
    avg_rating DECIMAL(4,2)
);
"""


def create_dw_schema():
    print("\n[DW] Creating DW schema...")
    conn = mysql_conn()
    cur = conn.cursor()
    for stmt in DW_SQL.split(";"):
        s = stmt.strip()
        if s:
            cur.execute(s)
    conn.commit()
    cur.close()
    conn.close()
    print("[DW] DW schema created successfully.")


# ====================================
# DYNAMODB SCAN
# ====================================

def scan_dynamodb(table_name):
    print(f"\n[DDB] Scanning DynamoDB table: {table_name} ...")
    t0 = time.time()

    table = dynamodb.Table(table_name)
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    print(f"[DDB] Finished scanning {table_name}: {len(items):,} rows. Time: {time.time()-t0:.2f}s")
    return items


# ====================================
# MYSQL EXTRACT
# ====================================
def extract_customers():
    print("\n[MYSQL] Extracting customers...")
    t0 = time.time()
    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM customers")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print(f"[MYSQL] Loaded customers: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")
    return rows


def extract_products():
    print("\n[MYSQL] Extracting products...")
    t0 = time.time()
    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM products")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print(f"[MYSQL] Loaded products: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")
    return rows


def extract_orders_with_shipments():
    print("\n[MYSQL] Extracting orders + shipments...")
    t0 = time.time()

    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()

    # Load orders
    cur.execute("SELECT * FROM orders")
    orders = cur.fetchall()

    # Load shipments, indexed by shipment_id
    cur.execute("SELECT * FROM shipments")
    shipments = {s["shipment_id"]: s for s in cur.fetchall()}

    cur.close()
    conn.close()

    final = []

    for o in orders:

        order_id = o["Order_ID"]
        customer_id = o["Customer_ID"]
        order_amount = o["Total_amount"]
        shipment_id = o["Shipment_ID"]       # matches your schema
        order_date = o["Order_date"]

        # Match shipment record
        ship = shipments.get(shipment_id)

        if ship:
            # Use shipment's ship_date as the authoritative order_date
            order_date_final = ship["ship_date"]
        else:
            order_date_final = order_date

        final.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": None,             # no product info available in your schema
            "order_date": order_date_final,
            "order_amount": order_amount,
            "shipment_id": shipment_id
        })

    print(f"[MYSQL] Final merged orders: {len(final):,} rows. Time: {time.time()-t0:.2f}s")
    return final


# ====================================
# TRANSFORM: SESSIONS
# ====================================
def aggregate_sessions(session_events):
    print("\n[TRANSFORM] Aggregating session events into sessions...")
    t0 = time.time()

    grouped = defaultdict(list)
    for evt in session_events:
        grouped[evt["session_id"]].append(evt)

    sessions = []

    for sid, events in grouped.items():
        customer_id = events[0]["customer_id"]

        timestamps = [
            datetime.fromisoformat(e["event_time"].replace("Z",""))
            for e in events
        ]

        session_start = min(timestamps)
        session_end = max(timestamps)

        view_count = sum(1 for e in events if e.get("event_type") == "view")
        cart_count = sum(1 for e in events if e.get("event_type") == "cart")
        purchase_count = sum(1 for e in events if e.get("event_type") == "purchase")

        total_value = sum(float(e.get("price", 0)) for e in events if "price" in e)

        sessions.append({
            "session_id": sid,
            "customer_id": customer_id,
            "session_start": session_start,
            "session_end": session_end,
            "event_count": len(events),
            "view_count": view_count,
            "cart_count": cart_count,
            "purchase_count": purchase_count,
            "total_value": total_value
        })

    print(f"[TRANSFORM] Sessions aggregated: {len(sessions):,}. Time: {time.time()-t0:.2f}s")
    return sessions


# ====================================
# LOAD DIM_CUSTOMER
# ====================================
def load_dim_customer(rows):
    print("\n[LOAD] dim_customer ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_customer
        (customer_id, full_name, email, signup_date, country, state, city, prime_flag)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE full_name=VALUES(full_name)
    """

    data = [
        (
            r["customer_id"], r["full_name"], r["email"], r["signup_date"],
            r["country"], r["state"], r["city"], r["prime_flag"]
        )
        for r in rows
    ]

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] dim_customer loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")


# ====================================
# LOAD DIM_PRODUCT
# ====================================
def load_dim_product(rows):
    print("\n[LOAD] dim_product ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_product
        (product_id, product_name, brand, category, price)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE 
            product_name = VALUES(product_name),
            brand = VALUES(brand),
            category = VALUES(category),
            price = VALUES(price)
    """

    data = []
    for r in rows:
        data.append((
            r["Product_ID"],
            r["Product_name"],
            r["Brand"],
            r["Category_ID"],
            r["Price"]
        ))

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] dim_product loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")


# ====================================
# LOAD DIM_SESSION
# ====================================
def load_dim_session(rows):
    print("\n[LOAD] dim_session ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_session
        (session_id, customer_id, session_start, session_end,
         event_count, view_count, cart_count, purchase_count, total_value)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE event_count=VALUES(event_count)
    """

    data = [
        (
            r["session_id"], r["customer_id"], r["session_start"], r["session_end"],
            r["event_count"], r["view_count"], r["cart_count"],
            r["purchase_count"], r["total_value"]
        )
        for r in rows
    ]

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] dim_session loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")


# ====================================
# LOAD DIM_REVIEW
# ====================================
def load_dim_review(rows):
    print("\n[LOAD] dim_review ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_review
        (review_id, customer_id, product_id, rating,
         review_title, review_text, review_timestamp, verified_purchase)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE rating=VALUES(rating)
    """

    data = [
        (
            r["review_id"], r["customer_id"], r["product_id"], int(r["rating"]),
            r.get("review_title",""), r.get("review_text",""),
            r["review_timestamp"], int(r.get("verified_purchase",0))
        )
        for r in rows
    ]

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] dim_review loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")


# ====================================
# LOAD FACT ORDERS
# ====================================
def load_fact_orders(rows):
    print("\n[LOAD] fact_orders ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO fact_orders
        (order_id, customer_id, product_id, order_date, order_amount, shipment_id)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE order_amount=VALUES(order_amount)
    """

    data = [
        (
            r["order_id"], r["customer_id"], r["product_id"],
            r["order_date"], r["order_amount"], r["shipment_id"]
        )
        for r in rows
    ]

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] fact_orders loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")


# ====================================
# BUILD FACT CUSTOMER ACTIVITY
# ====================================
def build_fact_customer_activity(sessions, reviews):
    print("\n[TRANSFORM] Building fact_customer_activity...")
    t0 = time.time()

    agg = defaultdict(lambda: {
        "session_count": 0,
        "review_count": 0,
        "view_count": 0,
        "cart_count": 0,
        "purchase_count": 0,
        "total_session_value": 0,
        "rating_sum": 0,
        "rating_cnt": 0
    })

    for s in sessions:
        key = (s["customer_id"], s["session_start"].date())
        agg[key]["session_count"] += 1
        agg[key]["view_count"] += s["view_count"]
        agg[key]["cart_count"] += s["cart_count"]
        agg[key]["purchase_count"] += s["purchase_count"]
        agg[key]["total_session_value"] += s["total_value"]

    for r in reviews:
        dt = datetime.fromisoformat(r["review_timestamp"].replace("Z",""))
        key = (r["customer_id"], dt.date())
        agg[key]["review_count"] += 1
        agg[key]["rating_sum"] += int(r["rating"])
        agg[key]["rating_cnt"] += 1

    rows = []
    for (cust, date_id), v in agg.items():
        avg_rating = (
            v["rating_sum"] / v["rating_cnt"] if v["rating_cnt"] > 0 else None
        )
        rows.append({
            "customer_id": cust,
            "date_id": date_id,
            "session_count": v["session_count"],
            "review_count": v["review_count"],
            "view_count": v["view_count"],
            "cart_count": v["cart_count"],
            "purchase_count": v["purchase_count"],
            "total_session_value": v["total_session_value"],
            "avg_rating": avg_rating
        })

    print(f"[TRANSFORM] fact_customer_activity built: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")
    return rows


# ====================================
# LOAD FACT CUSTOMER ACTIVITY
# ====================================
def load_fact_customer_activity(rows):
    print("\n[LOAD] fact_customer_activity ...")
    t0 = time.time()

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()

    sql = """
        INSERT INTO fact_customer_activity
        (customer_id, date_id, session_count, review_count,
         view_count, cart_count, purchase_count,
         total_session_value, avg_rating)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data = [
        (
            r["customer_id"], r["date_id"], r["session_count"], r["review_count"],
            r["view_count"], r["cart_count"], r["purchase_count"],
            r["total_session_value"], r["avg_rating"]
        )
        for r in rows
    ]

    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[LOAD] fact_customer_activity loaded: {len(rows):,} rows. Time: {time.time()-t0:.2f}s")

def extract_shipments():
    print("[MYSQL] Extracting shipments...")

    sql = "SELECT * FROM shipments"
    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"[MYSQL] Loaded shipments: {len(rows):,} rows.")
    return rows


# ============================================================
#  DIM_SHIPMENT_FEATURES (core of predictive application)
# ============================================================

def create_dim_shipment_features_table():
    sql = """
    CREATE TABLE IF NOT EXISTS dim_shipment_features (
        shipment_id              VARCHAR(64) PRIMARY KEY,
        order_id                 VARCHAR(64),
        customer_id              VARCHAR(64),
        warehouse_id             VARCHAR(20),
        region                   VARCHAR(20),
        carrier                  VARCHAR(50),
        date_key                 DATE,
        carrier_delay_rate_30d   DECIMAL(6,4),
        carrier_fail_rate_30d    DECIMAL(6,4),
        carrier_lost_rate_30d    DECIMAL(6,4),
        warehouse_load_index     DECIMAL(6,3),
        route_difficulty_score   DECIMAL(6,3),
        precipitation_mm         DECIMAL(10,3),
        wind_speed_kmh           DECIMAL(10,3),
        traffic_congestion_score DECIMAL(10,3),
        is_holiday               TINYINT,
        ship_date                DATE,
        estimated_delivery       DATE,
        distance_km              INT,
        delivery_type            VARCHAR(20),
        delivery_status          VARCHAR(20),
        feature_timestamp        DATETIME
    );
    """
    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def extract_dim_environment():
    print("[MYSQL] Extracting dim_environment...")
    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM dim_environment")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def extract_dim_operational():
    conn = mysql_conn(OLTP_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM dim_operational")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def build_dim_shipment_features(shipments, dim_env, dim_op):
    """
    shipments: list of dicts (already extracted earlier in your ETL)
    dim_env: list of dicts
    dim_op: list of dicts
    """

    env_map = {(row["date_key"], row["region"]): row for row in dim_env}
    op_map = {(row["carrier"], row["warehouse_id"]): row for row in dim_op}

    out = []

    for s in shipments:
        key_env = (s["ship_date"], s["warehouse_id"])
        key_op = (s["carrier"], s["warehouse_id"])

        e = env_map.get(key_env)
        o = op_map.get(key_op)

        out.append({
            "shipment_id": s["shipment_id"],
            "order_id": s["order_id"],
            "customer_id": s["customer_id"],
            "warehouse_id": s["warehouse_id"],
            "region": e["region"] if e else None,
            "carrier": s["carrier"],
            "date_key": s["ship_date"],
            "carrier_delay_rate_30d": o["carrier_delay_rate_30d"] if o else None,
            "carrier_fail_rate_30d": o["carrier_fail_rate_30d"] if o else None,
            "carrier_lost_rate_30d": o["carrier_lost_rate_30d"] if o else None,
            "warehouse_load_index": o["warehouse_load_index"] if o else None,
            "route_difficulty_score": o["route_difficulty_score"] if o else None,
            "precipitation_mm": e["precipitation_mm"] if e else None,
            "wind_speed_kmh": e["wind_speed_kmh"] if e else None,
            "traffic_congestion_score": e["traffic_congestion_score"] if e else None,
            "is_holiday": e["is_holiday"] if e else None,
            "ship_date": s["ship_date"],
            "estimated_delivery": s["estimated_delivery"],
            "distance_km": s["distance_km"],
            "delivery_type": s["delivery_type"],
            "delivery_status": s["delivery_status"],
            "feature_timestamp": datetime.utcnow()
        })

    return out


def load_dim_shipment_features(rows):
    sql = """
    INSERT INTO dim_shipment_features (
        shipment_id, order_id, customer_id, warehouse_id, region,
        carrier, date_key,
        carrier_delay_rate_30d, carrier_fail_rate_30d, carrier_lost_rate_30d,
        warehouse_load_index, route_difficulty_score,
        precipitation_mm, wind_speed_kmh, traffic_congestion_score, is_holiday,
        ship_date, estimated_delivery, distance_km, delivery_type,
        delivery_status, feature_timestamp
    ) VALUES (
        %s,%s,%s,%s,%s,
        %s,%s,
        %s,%s,%s,
        %s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s
    )
    """

    data = [
        (
            r["shipment_id"], r["order_id"], r["customer_id"], r["warehouse_id"], r["region"],
            r["carrier"], r["date_key"],
            r["carrier_delay_rate_30d"], r["carrier_fail_rate_30d"], r["carrier_lost_rate_30d"],
            r["warehouse_load_index"], r["route_difficulty_score"],
            r["precipitation_mm"], r["wind_speed_kmh"], r["traffic_congestion_score"], r["is_holiday"],
            r["ship_date"], r["estimated_delivery"], r["distance_km"], r["delivery_type"],
            r["delivery_status"], r["feature_timestamp"]
        )
        for r in rows
    ]

    conn = mysql_conn(DW_SCHEMA)
    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

# ====================================
# MAIN EXECUTION
# ====================================
def run_etl():
    print("\n==================== ETL START ====================")

    #create_dw_schema()

    #customers = extract_customers()
    #products = extract_products()
    #orders = extract_orders_with_shipments()

    #session_events = scan_dynamodb(SESSION_TABLE)
    #review_items = scan_dynamodb(REVIEWS_TABLE)

    #sessions = aggregate_sessions(session_events)

    #load_dim_customer(customers)
    #load_dim_product(products)
    #load_dim_session(sessions)
    #load_dim_review(review_items)
    #load_fact_orders(orders)
    
    print("[DIM_SHIPMENT_FEATURES] Creating table...")
    create_dim_shipment_features_table()

    print("[DIM_SHIPMENT_FEATURES] Extracting dim_environment...")
    dim_env = extract_dim_environment()

    print("[DIM_SHIPMENT_FEATURES] Extracting dim_operational...")
    dim_op = extract_dim_operational()

    print("[DIM_SHIPMENT_FEATURES] Extracting shipments...")
    shipments = extract_shipments()

    print("[DIM_SHIPMENT_FEATURES] Building feature dataset...")
    dim_ship = build_dim_shipment_features(shipments, dim_env, dim_op)

    print(f"[DIM_SHIPMENT_FEATURES] Rows built: {len(dim_ship)}")

    print("[DIM_SHIPMENT_FEATURES] Loading into MySQL...")
    
    load_dim_shipment_features(dim_ship)
    print("[DIM_SHIPMENT_FEATURES] Completed.")

    print("\n==================== ETL COMPLETED ====================\n")


if __name__ == "__main__":
    run_etl()
