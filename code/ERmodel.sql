CREATE DATABASE IF NOT EXISTS ermodel;
USE ermodel;

/* ---------- CUSTOMER ---------- */
CREATE TABLE customer (
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

/* ---------- PRODUCT DIMENSIONS ---------- */

CREATE TABLE brand (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(100) UNIQUE
);

CREATE TABLE category (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255)
);

/* ---------- PRODUCT FACT ---------- */

CREATE TABLE product (
    product_id CHAR(36) PRIMARY KEY,
    product_name VARCHAR(255),
    category_id INT,
    brand_id INT,
    price DECIMAL(10,2),
    discount DECIMAL(5,2),
    stock INT,
    dimension VARCHAR(50),
    weight DECIMAL(10,2),
    seller_id CHAR(36),
    compliance_flag TINYINT,
    updated_at DATETIME,
    FOREIGN KEY (category_id) REFERENCES category(category_id),
    FOREIGN KEY (brand_id) REFERENCES brand(brand_id)
);

/* ---------- ORDERS ---------- */

CREATE TABLE orders (
    order_id CHAR(36) PRIMARY KEY,
    customer_id CHAR(36),
    order_date DATE,
    shipping_address VARCHAR(255),
    payment_id CHAR(36),
    total_amount DECIMAL(10,2),
    order_status VARCHAR(50),
    shipment_id CHAR(36),
    updated_at DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

/* ---------- SHIPMENTS ---------- */

CREATE TABLE shipment (
    shipment_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(36),
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
    order_code VARCHAR(64),

    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),

    INDEX (customer_id)
);

/* INSERT DATA FROM AMAZON DB */

/* ---------- CUSTOMER ---------- */
INSERT INTO customer
SELECT *
FROM amazon.customers;

/* ---------- BRAND ---------- */
INSERT INTO brand (brand_name)
SELECT DISTINCT brand
FROM amazon.products
WHERE brand IS NOT NULL AND brand <> '';

/* ---------- CATEGORY ---------- */
INSERT INTO category (category_id, category_name)
SELECT DISTINCT category_id, category_id
FROM amazon.products
WHERE category_id IS NOT NULL;

/* ---------- PRODUCT ---------- */
INSERT INTO product (
    product_id, product_name, category_id, brand_id,
    price, discount, stock, dimension, weight,
    seller_id, compliance_flag, updated_at
)
SELECT
    p.product_id,
    p.product_name,
    p.category_id,
    b.brand_id,
    p.price,
    p.discount,
    p.stock,
    p.dimension,
    p.weight,
    p.seller_id,
    p.compliance_flag,
    CASE
        WHEN p.updated_at IS NULL OR TRIM(p.updated_at) = '' OR p.updated_at LIKE '0000%' 
        THEN NULL
        ELSE p.updated_at
    END
FROM amazon.products p
LEFT JOIN brand b ON p.brand = b.brand_name;

/* ---------- ORDERS ---------- */
INSERT INTO orders (
    order_id, customer_id, order_date, shipping_address,
    payment_id, total_amount, order_status, shipment_id, updated_at
)
SELECT
    order_id,
    customer_id,
    order_date,
    shipping_address_id,
    payment_id,
    total_amount,
    order_status,
    shipment_id,
    updated_at
FROM amazon.orders;

/* ---------- SHIPMENT ---------- */
INSERT INTO shipment
SELECT *
FROM amazon.shipments;

