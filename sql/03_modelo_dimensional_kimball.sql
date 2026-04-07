/*
=====================================================================
  OLIST E-COMMERCE — MODELO DIMENSIONAL KIMBALL (OLAP)
  Base de datos: OlistEcommerce
  
  Modelo Estrella con 3 Fact Tables + 4 Dimensiones.
  Se separaron las tablas de hechos por granularidad diferente
  (ventas, pagos, reviews) para evitar productos cartesianos
  al unir tablas con relaciones N:M.
=====================================================================
*/

USE OlistEcommerce;
GO

-- =============================================
-- 1. CREACIÓN DEL ESQUEMA ANALÍTICO
-- =============================================

CREATE SCHEMA analytics;
GO

-- =============================================
-- 2. DIMENSIONES
-- =============================================

-- Dim_Customer (99,441 registros — 1:1 con PK)
CREATE OR ALTER VIEW analytics.dim_customer AS
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM customers;
GO

-- Dim_Product (32,951 registros — 1:1 con PK)
-- LEFT JOIN para preservar 610 productos sin categoría registrada
CREATE OR ALTER VIEW analytics.dim_product AS
SELECT
    p.product_id,
    p.product_category_name,
    c.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM products p
LEFT JOIN product_category_name_translation c
    ON p.product_category_name = c.product_category_name;
GO

-- Dim_Seller (3,095 registros — 1:1 con PK)
CREATE OR ALTER VIEW analytics.dim_seller AS
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM sellers;
GO

/*
  Dim_Calendar: Creada directamente en Power BI (Power Query)
  ya que depende del rango dinámico de fechas del modelo.
  Incluye: Año, Trimestre, Mes, Nombre de Mes, Día de Semana.
*/

-- =============================================
-- 3. FACT TABLES
-- =============================================

/*
  ¿Por qué 3 fact tables separadas?
  
  - orders + order_items  → Granularidad: 1 fila por ítem vendido (112,650 registros)
  - orders + order_payments → Granularidad: 1 fila por pago registrado (103,886 registros)
  - orders + order_reviews  → Granularidad: 1 fila por review emitida (99,224 registros)
  
  Un solo JOIN entre las 3 generaría un producto cartesiano (una orden con 3 ítems,
  2 pagos y 1 review produciría 6 filas en vez de los valores reales).
  Esta separación permite que Power BI calcule métricas correctas sobre cada tabla
  sin necesidad de relaciones bidireccionales.
*/

-- Fact_Ventas: orders + order_items
-- Incluye columnas calculadas de logística (días de entrega, retraso)
CREATE OR ALTER VIEW analytics.fact_order_items AS
SELECT
    o.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.order_status,
    o.order_purchase_timestamp AS purchase_date,
    o.order_approved_at,
    oi.shipping_limit_date,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    -- Métricas de entrega calculadas
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_days,
    DATEDIFF(DAY, o.order_approved_at, o.order_delivered_carrier_date) AS seller_to_courier_delivery_days,
    DATEDIFF(DAY, o.order_delivered_carrier_date, o.order_delivered_customer_date) AS courier_to_customer_delivery_days,
    -- Flag de retraso (CAST para comparar solo fechas, ignorando horas)
    CASE
        WHEN CAST(o.order_delivered_customer_date AS DATE) > CAST(o.order_estimated_delivery_date AS DATE) 
        THEN 'Sí' ELSE 'No'
    END AS is_late,
    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN DATEDIFF(DAY, o.order_estimated_delivery_date, o.order_delivered_customer_date)
        ELSE 0
    END AS delay_days,
    -- Montos
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS subtotal
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id;
GO

-- Fact_Pagos: orders + order_payments
CREATE OR ALTER VIEW analytics.fact_order_payments AS
SELECT
    o.order_id,
    op.payment_sequential,
    op.payment_type,
    op.payment_installments,
    op.payment_value,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp AS purchase_date,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM orders o
LEFT JOIN order_payments op ON o.order_id = op.order_id;
GO

-- Fact_Reviews: orders + order_reviews
-- Incluye clasificación de tipo de logística (envío simple vs dividido)
CREATE OR ALTER VIEW analytics.fact_order_reviews AS
WITH sellers_por_orden AS (
    SELECT
        order_id,
        COUNT(DISTINCT seller_id) AS cantidad_vendedores
    FROM order_items
    GROUP BY order_id
)
SELECT
    r.review_id,
    o.order_id,
    o.customer_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    o.order_status,
    o.order_purchase_timestamp AS purchase_date,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_days,
    CASE
        WHEN CAST(o.order_delivered_customer_date AS DATE) > CAST(o.order_estimated_delivery_date AS DATE) 
        THEN 'Sí' ELSE 'No'
    END AS is_late,
    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN DATEDIFF(DAY, o.order_estimated_delivery_date, o.order_delivered_customer_date)
        ELSE 0
    END AS delay_days,
    CASE
        WHEN s.cantidad_vendedores = 1 THEN 'Envío Simple (1 Vendedor)'
        WHEN s.cantidad_vendedores > 1 THEN 'Envío Dividido (+ de 1 Vendedor)'
        ELSE 'Sin Vendedor / Cancelado'
    END AS tipo_logistica
FROM orders o
LEFT JOIN order_reviews r ON o.order_id = r.order_id
LEFT JOIN sellers_por_orden s ON o.order_id = s.order_id;
GO

-- =============================================
-- 4. CONSULTAS ANALÍTICAS PARA EL DASHBOARD
-- =============================================

/*
  Las siguientes consultas se usaron para validar los cálculos
  de las medidas DAX y generar los hallazgos del dashboard.
*/

-- HALLAZGO #1: ¿Los retrasos afectan la calificación?
-- Resultado: Score con retraso = 2.27 vs sin retraso = 4.21
WITH review_table AS (
    SELECT
        order_id,
        AVG(review_score * 1.00) AS avg_review_score,
        MAX(is_late) AS is_late
    FROM analytics.fact_order_reviews
    GROUP BY order_id
)
SELECT
    'Entregas con Retraso' AS tipo_entrega,
    CAST(AVG(avg_review_score) AS DECIMAL(3,2)) AS score_promedio
FROM review_table
WHERE is_late = 'Sí'
UNION ALL
SELECT
    'Entregas sin Retraso',
    CAST(AVG(avg_review_score) AS DECIMAL(3,2))
FROM review_table
WHERE is_late = 'No';

-- HALLAZGO #2: ¿La fricción operativa (envío dividido) destruye la satisfacción?
-- Resultado: Envío simple = 4.12 vs Envío dividido = 2.85
WITH sellers_por_orden AS (
    SELECT
        order_id,
        COUNT(DISTINCT seller_id) AS cantidad_vendedores
    FROM analytics.fact_order_items
    GROUP BY order_id
),
clasificacion_envios AS (
    SELECT
        r.order_id,
        r.review_score,
        CASE
            WHEN s.cantidad_vendedores = 1 THEN 'Envío Simple (1 Vendedor)'
            WHEN s.cantidad_vendedores > 1 THEN 'Envío Dividido (+ de 1 Vendedor)'
        END AS tipo_logistica
    FROM analytics.fact_order_reviews r
    INNER JOIN sellers_por_orden s ON r.order_id = s.order_id
)
SELECT
    tipo_logistica,
    COUNT(order_id) AS volumen_ordenes,
    CAST(AVG(review_score * 1.00) AS DECIMAL(3,2)) AS score_promedio
FROM clasificacion_envios
WHERE tipo_logistica IS NOT NULL
GROUP BY tipo_logistica
ORDER BY tipo_logistica ASC;

-- HALLAZGO #3: Días de entrega desglosados (Vendedor→Courier vs Courier→Cliente)
-- Resultado: Con retraso → vendedor 5.43 días + courier 27.87 días (causa raíz: courier)
WITH delivery_days_table AS (
    SELECT
        order_id,
        MAX(order_status) AS order_status,
        MAX(seller_to_courier_delivery_days) AS seller_to_courier_delivery_days,
        MAX(courier_to_customer_delivery_days) AS courier_to_customer_delivery_days,
        MAX(is_late) AS is_late,
        MAX(delay_days) AS delay_days
    FROM analytics.fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY order_id
)
SELECT
    is_late,
    CAST(AVG(seller_to_courier_delivery_days * 1.00) AS DECIMAL(5,2)) AS avg_seller_to_courier,
    CAST(AVG(courier_to_customer_delivery_days * 1.00) AS DECIMAL(5,2)) AS avg_courier_to_customer
FROM delivery_days_table
GROUP BY is_late;

-- HALLAZGO #4: Vendedores con mayor tasa de retraso (Hall of Shame)
-- Filtro: mínimo 30 pedidos para significancia estadística
WITH seller_orders AS (
    SELECT
        order_id,
        seller_id,
        is_late,
        MAX(delay_days) AS delay_days
    FROM analytics.fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY order_id, seller_id, is_late
)
SELECT
    seller_id,
    COUNT(*) AS total_pedidos,
    SUM(CASE WHEN is_late = 'Sí' THEN 1 ELSE 0 END) AS pedidos_con_retraso,
    CAST(SUM(CASE WHEN is_late = 'Sí' THEN 1 ELSE 0 END) * 100.00 / COUNT(*) AS DECIMAL(5,2)) AS tasa_retraso,
    CAST(AVG(CASE WHEN is_late = 'Sí' THEN delay_days * 1.00 ELSE NULL END) AS DECIMAL(6,2)) AS dias_promedio_retraso
FROM seller_orders
GROUP BY seller_id
HAVING COUNT(*) >= 30
ORDER BY tasa_retraso DESC;

-- Review promedio por categoría de producto
WITH review_table AS (
    SELECT
        order_id,
        AVG(review_score * 1.00) AS avg_review_score
    FROM order_reviews
    GROUP BY order_id
),
base_data AS (
    SELECT
        oi.order_id,
        r.avg_review_score,
        c.product_category_name_english
    FROM analytics.fact_order_items oi
    LEFT JOIN review_table r ON oi.order_id = r.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    INNER JOIN product_category_name_translation c ON p.product_category_name = c.product_category_name
)
SELECT
    product_category_name_english AS categoria,
    CAST(AVG(avg_review_score) AS DECIMAL(3,2)) AS score_promedio
FROM base_data
WHERE avg_review_score IS NOT NULL
GROUP BY product_category_name_english
ORDER BY score_promedio;

-- Tasa de retraso por categoría
WITH fact_ventas_agregada AS (
    SELECT
        order_id,
        COUNT(*) AS qty,
        product_id,
        is_late,
        SUM(subtotal) AS subtotal
    FROM analytics.fact_order_items
    GROUP BY order_id, product_id, is_late
),
orders_table AS (
    SELECT
        oi.order_id,
        c.product_category_name_english AS categoria,
        oi.is_late
    FROM fact_ventas_agregada oi
    INNER JOIN products p ON oi.product_id = p.product_id
    INNER JOIN product_category_name_translation c ON p.product_category_name = c.product_category_name
)
SELECT
    categoria,
    COUNT(*) AS total_pedidos,
    SUM(CASE WHEN is_late = 'Sí' THEN 1 ELSE 0 END) AS pedidos_con_retraso,
    CAST(SUM(CASE WHEN is_late = 'Sí' THEN 1.0 ELSE 0.0 END) * 100 / COUNT(*) AS DECIMAL(5,2)) AS tasa_retraso
FROM orders_table
GROUP BY categoria
ORDER BY tasa_retraso DESC;

-- Tasa de retraso por estado del comprador
SELECT
    c.customer_state,
    CAST(
        SUM(CASE WHEN CAST(o.order_delivered_customer_date AS DATE) > order_estimated_delivery_date THEN 1.0 ELSE 0.0 END)
        / SUM(CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN 1.0 ELSE 0.0 END)
    AS DECIMAL(5,4)) AS tasa_retraso
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY tasa_retraso DESC;

-- Ingresos totales por categoría
SELECT
    c.product_category_name_english AS categoria,
    SUM(oi.price) AS ingreso
FROM analytics.fact_order_items oi
INNER JOIN products p ON oi.product_id = p.product_id
INNER JOIN product_category_name_translation c ON p.product_category_name = c.product_category_name
GROUP BY c.product_category_name_english
ORDER BY ingreso DESC;

-- HALLAZGO #5: ¿Más cuotas equivalen a un ticket más alto?
-- Resultado: Sí. 1 cuota = R$96 promedio → 11+ cuotas = R$358 promedio
WITH payments_table AS (
    SELECT
        *,
        CASE
            WHEN payment_installments = 1 THEN '1 cuota (contado)'
            WHEN payment_installments >= 2 AND payment_installments <= 3 THEN '2-3 cuotas'
            WHEN payment_installments >= 4 AND payment_installments <= 6 THEN '4-6 cuotas'
            WHEN payment_installments >= 7 AND payment_installments <= 10 THEN '7-10 cuotas'
            ELSE '11+ cuotas'
        END AS rango_cuotas
    FROM analytics.fact_order_payments
    WHERE payment_type = 'credit_card'
)
SELECT
    rango_cuotas,
    AVG(payment_value) AS ticket_promedio
FROM payments_table
GROUP BY rango_cuotas
ORDER BY ticket_promedio ASC;

-- Distribución de métodos de pago
WITH payments_table AS (
    SELECT
        order_id,
        payment_type,
        SUM(payment_value) AS payment_value
    FROM analytics.fact_order_payments
    GROUP BY order_id, payment_type
)
SELECT
    payment_type,
    COUNT(*) AS total_ordenes
FROM payments_table
GROUP BY payment_type
ORDER BY total_ordenes DESC;