CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_key       BIGSERIAL PRIMARY KEY,
  source_customer_id BIGINT UNIQUE NOT NULL,
  customer_name      TEXT,
  country            TEXT,
  age                INT,
  email              TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_seller (
  seller_key       BIGSERIAL PRIMARY KEY,
  source_seller_id BIGINT UNIQUE NOT NULL,
  seller_name      TEXT,
  country          TEXT,
  email            TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_key       BIGSERIAL PRIMARY KEY,
  source_product_id BIGINT UNIQUE NOT NULL,
  product_name      TEXT,
  category          TEXT,
  price             NUMERIC(12,2),
  rating            NUMERIC(3,1),
  reviews           INT
);

CREATE TABLE IF NOT EXISTS dw.dim_store (
  store_key   BIGSERIAL PRIMARY KEY,
  store_name  TEXT UNIQUE NOT NULL,
  city        TEXT,
  country     TEXT,
  email       TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_supplier (
  supplier_key   BIGSERIAL PRIMARY KEY,
  supplier_name  TEXT UNIQUE NOT NULL,
  country        TEXT,
  email          TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key  BIGSERIAL PRIMARY KEY,
  sale_date DATE UNIQUE NOT NULL,
  year      INT NOT NULL,
  month     INT NOT NULL,
  day       INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
  fact_key         BIGSERIAL PRIMARY KEY,
  source_sale_id   BIGINT UNIQUE NOT NULL,

  customer_key     BIGINT REFERENCES dw.dim_customer(customer_key),
  seller_key       BIGINT REFERENCES dw.dim_seller(seller_key),
  product_key      BIGINT REFERENCES dw.dim_product(product_key),
  store_key        BIGINT REFERENCES dw.dim_store(store_key),
  supplier_key     BIGINT REFERENCES dw.dim_supplier(supplier_key),
  date_key         BIGINT REFERENCES dw.dim_date(date_key),

  sale_quantity    INT,
  sale_total_price NUMERIC(14,2)
);

CREATE INDEX IF NOT EXISTS idx_fact_date    ON dw.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod    ON dw.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_cust    ON dw.fact_sales(customer_key);
