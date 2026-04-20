CREATE DATABASE amazon;
USE amazon;

CREATE TABLE customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    signup_date DATE,
    country VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255),
    zip_code VARCHAR(20),
    address_line1 VARCHAR(255),
    prime_flag TINYINT,
    preferred_language VARCHAR(20),
    marketing_opt_in TINYINT,
    account_status VARCHAR(50),
    birth_year INT,
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE shipments (
    shipment_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64),
    customer_id VARCHAR(64),
    carrier VARCHAR(50),
    warehouse_id VARCHAR(10),
    ship_date DATE,
    estimated_delivery DATE,
    actual_delivery DATE,
    delivery_type VARCHAR(50),
    distance_km INT,
    package_weight_kg DECIMAL(10,2),
    package_volume_cm3 INT,
    tracking_number VARCHAR(50),
    delivery_status VARCHAR(50),
    failed_attempts INT,
    delivery_delay_flag TINYINT,
    delay_reason_code VARCHAR(50),
    return_flag TINYINT,
    created_at DATETIME,
    updated_at DATETIME,

    INDEX (customer_id)
);

CREATE TABLE products (
    product_id CHAR(36) PRIMARY KEY,
    product_name VARCHAR(255),
    category_id INT,
    brand VARCHAR(100),
    price DECIMAL(10,2),
    discount DECIMAL(5,2),
    stock INT,
    dimension VARCHAR(50),
    weight DECIMAL(10,2),
    seller_id CHAR(36),
    compliance_flag TINYINT,
    updated_at DATETIME
);

CREATE TABLE orders (
    order_id CHAR(36) PRIMARY KEY,
    customer_id CHAR(36),
    order_date DATE,
    shipping_address_id CHAR(36),
    payment_id CHAR(36),
    total_amount DECIMAL(10,2),
    order_status VARCHAR(50),
    shipment_id CHAR(36),
    updated_at DATETIME
);

SHOW VARIABLES LIKE 'local_infile';

LOAD DATA LOCAL INFILE 'customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'shipments.csv'
INTO TABLE shipments
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
IGNORE 1 LINES;




