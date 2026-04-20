CREATE DATABASE IF NOT EXISTS amazon_dw;
USE amazon_dw;


CREATE EXTERNAL TABLE fact_events (
    event_time        timestamp,
    event_type        string,
    product_id        string,
    category_id       string,
    category_code     string,
    brand             string,
    price             double,
    user_session      string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "skip.header.line.count" = "1"
)
LOCATION 's3://msba-bigdata-groupproject/data/ecommerce/';

CREATE TABLE dim_date AS
SELECT
    DISTINCT
    from_unixtime(unix_timestamp(event_time), 'yyyy-MM-dd') AS date_id,
    year(event_time)         AS year,
    month(event_time)        AS month,
    day(event_time)          AS day,
    weekofyear(event_time)   AS week_of_year,
    date_format(event_time, 'EEEE') AS weekday,
    case when date_format(event_time, 'u') in ('6','7') then 1 else 0 end AS is_weekend
FROM fact_events;

CREATE TABLE dim_event_type AS
SELECT
    DISTINCT event_type AS event_type_id,
    event_type AS description
FROM fact_events;

SELECT COUNT(*) FROM fact_events;

SELECT COUNT(*) 
FROM fact_events
WHERE event_type = 'purchase';

SELECT brand, COUNT(*) AS views
FROM fact_events
WHERE event_type = 'view'
GROUP BY brand
ORDER BY views DESC
LIMIT 20;

SELECT d.year, d.month, COUNT(*) AS events
FROM fact_events f
JOIN dim_date d
  ON from_unixtime(unix_timestamp(f.event_time), 'yyyy-MM-dd') = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

SELECT s.session_id, s.event_count, s.session_start_time, s.session_end_time
FROM dim_session s
ORDER BY s.event_count DESC
LIMIT 20;

SELECT e.event_type_id, COUNT(*) AS total_events
FROM fact_events f
JOIN dim_event_type e
  ON f.event_type = e.event_type_id
GROUP BY e.event_type_id
ORDER BY total_events DESC;
