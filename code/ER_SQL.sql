CREATE SCHEMA amazon;
USE amazon;

# customers
CREATE TABLE customers (
    customer_id        VARCHAR(36)  PRIMARY KEY,
    full_name          VARCHAR(255),
    email              VARCHAR(255),
    phone_number       VARCHAR(50),
    signup_date        DATE,
    country            VARCHAR(100),
    state              VARCHAR(100),
    city               VARCHAR(100),
    zip_code           VARCHAR(20),
    address_line1      VARCHAR(255),
    prime_flag         TINYINT(1),
    preferred_language VARCHAR(10),
    marketing_opt_in   TINYINT(1),
    account_status     VARCHAR(50),
    birth_year         INT,
    created_at         DATETIME,
    updated_at         DATETIME
);

# shipments
CREATE TABLE shipments (
    shipment_id          VARCHAR(36) PRIMARY KEY,
    order_code             VARCHAR(36),
    customer_id          VARCHAR(36),
    carrier              VARCHAR(100),
    warehouse_id         VARCHAR(20),
    ship_date            DATE,
    estimated_delivery   DATE,
    actual_delivery      DATE,
    delivery_type        VARCHAR(20),
    distance_km          INT,
    package_weight_kg    DECIMAL(10,2),
    package_volume_cm3   INT,
    tracking_number      VARCHAR(50),
    delivery_status      VARCHAR(50),
    failed_attempts      INT,
    delivery_delay_flag  TINYINT(1),
    delay_reason_code    VARCHAR(50),
    return_flag          TINYINT(1),
    created_at           DATETIME,
    updated_at           DATETIME,
    KEY idx_ship_customer (customer_id)
);

# products
CREATE TABLE products (
    Product_ID       VARCHAR(36) PRIMARY KEY,
    Product_name     VARCHAR(255),
    Category_ID      INT,
    Brand            VARCHAR(255),
    Price            DECIMAL(10,2),
    Discount         DECIMAL(5,2),
    Stock            INT,
    Dimension        VARCHAR(50),
    Weight           DECIMAL(10,2),
    Seller_ID        VARCHAR(36),
    Compliance_flag  TINYINT(1),
    Updated_at       DATETIME
);

# ordres
CREATE TABLE orders(
    Order_code            VARCHAR(36) PRIMARY KEY,
    Customer_ID         VARCHAR(36),
    Order_date          DATE,
    Shipping_address_ID VARCHAR(36),
    Payment_ID          VARCHAR(36),
    Total_amount        DECIMAL(10,2),
    Order_status        VARCHAR(50),
    Shipment_ID         VARCHAR(36),
    Updated_at          DATETIME,
    KEY idx_order_customer (Customer_ID),
    KEY idx_order_shipment (Shipment_ID)
);

LOAD DATA LOCAL INFILE '/Users/villanellechen/Desktop/BD project/customers.csv'
INTO TABLE customers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/villanellechen/Desktop/BD project/shipments.csv'
INTO TABLE shipments
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/villanellechen/Desktop/BD project/products.csv'
INTO TABLE products
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/villanellechen/Desktop/BD project/orders.csv'
INTO TABLE orders
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

select s.shipment_id
From shipments s
Join orders o
On s.shipment_id = o.shipment_id;

Describe orders;

SET SQL_SAFE_UPDATES = 0;

UPDATE shipments s
JOIN orders o
  ON s.shipment_id = o.Shipment_id
SET s.order_id = o.order_id;

Alter table shipments
modify column order_id VARCHAR(36) after shipment_id;

describe shipments;

Alter table shipments
modify column order_code VARCHAR(64) after updated_at;