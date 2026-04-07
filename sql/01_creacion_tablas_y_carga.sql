/*
=====================================================================
  OLIST E-COMMERCE — CREACIÓN DE TABLAS Y CARGA DE DATOS
  Base de datos: OlistEcommerce
  Dataset: Olist Brazilian E-Commerce de Kaggle
  Período: 2016 - 2018
=====================================================================
*/

-- =============================================
-- 1. CREACIÓN DE BASE DE DATOS
-- =============================================

CREATE DATABASE OlistEcommerce;
GO
USE OlistEcommerce;
GO

-- =============================================
-- 2. CREACIÓN DE TABLAS
-- =============================================

-- Tabla de clientes (99,441 registros)
CREATE TABLE customers (
    customer_id                VARCHAR(100) PRIMARY KEY,
    customer_unique_id         VARCHAR(100),
    customer_zip_code_prefix   VARCHAR(20),
    customer_city              VARCHAR(100),
    customer_state             VARCHAR(10)
);

-- Tabla de órdenes (99,441 registros)
CREATE TABLE orders (
    order_id                        VARCHAR(100) PRIMARY KEY,
    customer_id                     VARCHAR(100),
    order_status                    VARCHAR(35),
    order_purchase_timestamp        DATETIME,
    order_approved_at               DATETIME,
    order_delivered_carrier_date    DATETIME,
    order_delivered_customer_date   DATETIME,
    order_estimated_delivery_date   DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Tabla de productos (32,951 registros)
CREATE TABLE products (
    product_id                  VARCHAR(100) PRIMARY KEY,
    product_category_name       VARCHAR(100),
    product_name_lenght         INT,
    product_description_lenght  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT
);

-- Tabla de vendedores (3,095 registros)
CREATE TABLE sellers (
    seller_id               VARCHAR(100) PRIMARY KEY,
    seller_zip_code_prefix  VARCHAR(20),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(10)
);

-- Tabla de ítems por orden — PK compuesta (112,650 registros)
CREATE TABLE order_items (
    order_id             VARCHAR(100),
    order_item_id        INT,
    product_id           VARCHAR(100),
    seller_id            VARCHAR(100),
    shipping_limit_date  DATETIME,
    price                DECIMAL(10,2),
    freight_value        DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

-- Tabla de pagos por orden (103,886 registros)
CREATE TABLE order_payments (
    order_id              VARCHAR(100),
    payment_sequential    INT,
    payment_type          VARCHAR(30),
    payment_installments  INT,
    payment_value         DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Tabla de reviews por orden (99,224 registros)
CREATE TABLE order_reviews (
    review_id               VARCHAR(100),
    order_id                VARCHAR(100),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  VARCHAR(MAX),
    review_creation_date    DATETIME,
    review_answer_timestamp DATETIME,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Tabla de traducción de categorías (71 registros)
CREATE TABLE product_category_name_translation (
    product_category_name          VARCHAR(100) PRIMARY KEY,
    product_category_name_english  VARCHAR(100)
);

-- Tabla de geolocalización (1,000,163 registros)
CREATE TABLE geolocation (
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_lat             FLOAT,
    geolocation_lng             FLOAT,
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(10)
);

-- =============================================
-- 3. CARGA DE DATOS CON BULK INSERT
-- =============================================

BULK INSERT customers
FROM 'C:\olist\olist_data\olist_customers_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

BULK INSERT orders
FROM 'C:\olist\olist_data\olist_orders_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

BULK INSERT products
FROM 'C:\olist\olist_data\olist_products_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

/*
  NOTA: La carga de sellers falló en las filas 553 y 2990 (columna seller_state).
  Causa: Comas embebidas dentro del campo seller_city (ej. "rio de janeiro, rio de janeiro, brasil")
  que confunden al delimitador de BULK INSERT.
  Solución: Corrección manual del CSV (2 registros). Para volúmenes mayores se usaría Python + Pandas.
*/
BULK INSERT sellers
FROM 'C:\olist\olist_data\olist_sellers_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

BULK INSERT order_items
FROM 'C:\olist\olist_data\olist_order_items_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

BULK INSERT order_payments
FROM 'C:\olist\olist_data\olist_order_payments_dataset.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

/*
  NOTA: order_reviews fue la tabla más problemática.
  Causa: Saltos de línea en comentarios, textos > 255 caracteres y comas dentro de mensajes.
  Solución: Se usó la importación de Flat File de SSMS para crear una tabla temporal
  (olist_order_reviews_dataset) y luego se volcaron los datos a la tabla definitiva.
*/
INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_title,
                           review_comment_message, review_creation_date, review_answer_timestamp)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp
FROM olist_order_reviews_dataset;

BULK INSERT product_category_name_translation
FROM 'C:\olist\olist_data\product_category_name_translation.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

/*
  NOTA: La carga de geolocation falló en las filas 421040 y 698308 (misma causa: comas embebidas).
  Solución: Se limpió el CSV con Python + Pandas (olist_geolocation_dataset_clean.csv).
*/
BULK INSERT geolocation
FROM 'C:\olist\olist_data\olist_geolocation_dataset_clean.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- =============================================
-- 4. LIMPIEZA DE DATOS
-- =============================================

/*
  Problema: Los CSVs del dataset de Kaggle contienen comillas dobles (") embebidas
  en los campos de texto VARCHAR. BULK INSERT las importa literalmente.
  Solución: UPDATE + REPLACE para eliminar las comillas de cada tabla afectada.
*/

-- Limpieza de customers
UPDATE customers
SET customer_id = REPLACE(customer_id, '"', ''),
    customer_unique_id = REPLACE(customer_unique_id, '"', ''),
    customer_zip_code_prefix = REPLACE(customer_zip_code_prefix, '"', ''),
    customer_city = REPLACE(customer_city, '"', ''),
    customer_state = REPLACE(customer_state, '"', '');

-- Limpieza de orders
UPDATE orders
SET order_id = REPLACE(order_id, '"', ''),
    customer_id = REPLACE(customer_id, '"', '');

-- Limpieza de products
UPDATE products
SET product_id = REPLACE(product_id, '"', '');

-- Limpieza de sellers
UPDATE sellers
SET seller_id = REPLACE(seller_id, '"', ''),
    seller_zip_code_prefix = REPLACE(seller_zip_code_prefix, '"', ''),
    seller_city = REPLACE(seller_city, '"', '');

-- Limpieza de order_items
UPDATE order_items
SET order_id = REPLACE(order_id, '"', ''),
    product_id = REPLACE(product_id, '"', ''),
    seller_id = REPLACE(seller_id, '"', '');

-- Limpieza de order_payments
UPDATE order_payments
SET order_id = REPLACE(order_id, '"', '');

-- Se agregan 2 categorías que existían en products pero no en la tabla de traducción
INSERT INTO product_category_name_translation
    (product_category_name, product_category_name_english)
VALUES
    ('pc_gamer', 'pc_gamer'),
    ('portateis_cozinha_e_preparadores_de_alimentos', 'kitchen_portables_and_food_preparers');

-- =============================================
-- 5. VERIFICACIÓN FINAL
-- =============================================

SELECT 'customers'            AS tabla, COUNT(*) AS registros FROM customers
UNION ALL
SELECT 'orders',              COUNT(*) FROM orders
UNION ALL
SELECT 'order_items',         COUNT(*) FROM order_items
UNION ALL
SELECT 'order_payments',      COUNT(*) FROM order_payments
UNION ALL
SELECT 'order_reviews',       COUNT(*) FROM order_reviews
UNION ALL
SELECT 'products',            COUNT(*) FROM products
UNION ALL
SELECT 'sellers',             COUNT(*) FROM sellers
UNION ALL
SELECT 'geolocation',         COUNT(*) FROM geolocation
UNION ALL
SELECT 'category_translation', COUNT(*) FROM product_category_name_translation;

/*
  Resultado esperado:
  ┌──────────────────────┬───────────┐
  │ tabla                │ registros │
  ├──────────────────────┼───────────┤
  │ customers            │    99,441 │
  │ orders               │    99,441 │
  │ order_items           │   112,650 │
  │ order_payments        │   103,886 │
  │ order_reviews         │    99,224 │
  │ products              │    32,951 │
  │ sellers               │     3,095 │
  │ geolocation           │ 1,000,163 │
  │ category_translation  │        73 │
  └──────────────────────┴───────────┘
*/
