/*
=====================================================================
  OLIST E-COMMERCE — ANÁLISIS EXPLORATORIO DE DATOS (EDA)
  Base de datos: OlistEcommerce
  
  Este archivo contiene las consultas analíticas que responden
  preguntas de negocio sobre el marketplace Olist.
  Técnicas: JOINs multitablas, CTEs, Window Functions
  (LAG, DENSE_RANK, SUM OVER), subconsultas, CASE WHEN.
=====================================================================
*/

USE OlistEcommerce;
GO

-- =============================================
-- SECCIÓN 1: PANORAMA GENERAL DEL NEGOCIO
-- =============================================

-- 1. Distribución de pedidos por estado (status)
SELECT
    order_status AS estado_de_orden,
    COUNT(*) AS cantidad,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS porcentaje
FROM orders
GROUP BY order_status
ORDER BY COUNT(*) DESC;

-- 2. Rango temporal del dataset
SELECT
    MIN(order_purchase_timestamp) AS primera_compra,
    MAX(order_purchase_timestamp) AS ultima_compra,
    DATEDIFF(DAY, MIN(order_purchase_timestamp), MAX(order_purchase_timestamp)) AS dias_totales
FROM orders;

-- 3. Volumen de pedidos por mes
SELECT
    YEAR(order_purchase_timestamp) AS anio,
    MONTH(order_purchase_timestamp) AS mes,
    COUNT(*) AS total_pedidos
FROM orders
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY anio, mes;

-- 4. Estadísticas de precio y flete por ítem
SELECT
	MIN(price) AS precio_minimo,
	MAX(price) AS precio_maximo,
	CAST(AVG(price) AS DECIMAL(5,2)) AS precio_promedio,
	MIN(freight_value) AS flete_minimo,
	MAX(freight_value) AS flete_maximo,
	CAST(AVG(freight_value) AS DECIMAL(5,2)) AS flete_promedio
FROM order_items;

-- =============================================
-- SECCIÓN 2: ANÁLISIS DE MÉTODOS DE PAGO
-- =============================================

/*
  Una orden puede pagarse con múltiples métodos (ej. tarjeta + varios vouchers).
  La versión "MEJORADA" agrupa primero los pagos a nivel de orden para evitar
  contar los vouchers múltiples como transacciones separadas.
*/

-- 5. Distribución de métodos de pago (con lógica por pedido — versión corregida)
WITH pagos_agrupados_por_orden AS (
    SELECT
        order_id,
        payment_type,
        SUM(payment_value) AS monto_total_por_orden,
        MAX(payment_installments) AS cuotas
    FROM order_payments
    GROUP BY order_id, payment_type
)
SELECT 
    payment_type,
    COUNT(order_id) AS total_ordenes_por_metodo,
    CAST(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER() AS DECIMAL(5,2)) AS porcentaje,
    SUM(monto_total_por_orden) AS monto_total_por_metodo,
    CAST(AVG(monto_total_por_orden) AS DECIMAL(10,2)) AS ticket_promedio_por_metodo,
    CAST(AVG(cuotas*1.00) AS DECIMAL(3,2)) AS cuotas_promedio_por_metodo
FROM pagos_agrupados_por_orden
GROUP BY payment_type
ORDER BY monto_total_por_metodo DESC;

-- =============================================
-- SECCIÓN 3: ANÁLISIS DE SATISFACCIÓN
-- =============================================

-- 6. Distribución de review scores (agregado por orden para evitar duplicados)
WITH score_promedio_por_orden AS (
    SELECT
        order_id,
        CAST(ROUND(AVG(review_score * 1.0), 0) AS INT) AS puntuacion
    FROM order_reviews
    GROUP BY order_id
)
SELECT
    puntuacion,
    COUNT(*) AS total_ordenes,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS porcentaje
FROM score_promedio_por_orden
GROUP BY puntuacion
ORDER BY puntuacion DESC;

-- =============================================
-- SECCIÓN 4: ANÁLISIS DE REVENUE
-- =============================================

-- 7. Top 15 categorías por revenue (JOIN de 3 tablas)
SELECT TOP 15
    p.product_category_name AS categoria_pt,
    c.product_category_name_english AS categoria_en,
    SUM(o.price) AS revenue_total,
    CAST(SUM(o.price) * 100.0 / SUM(SUM(o.price)) OVER() AS DECIMAL(5,2)) AS porcentaje,
    CAST(AVG(o.price * 1.00) AS DECIMAL(10,2)) AS precio_medio_por_categoria
FROM order_items o
LEFT JOIN products p ON o.product_id = p.product_id
LEFT JOIN product_category_name_translation c ON p.product_category_name = c.product_category_name
GROUP BY c.product_category_name_english, p.product_category_name
ORDER BY revenue_total DESC;

-- 8. Ingreso mensual (solo pedidos entregados)
SELECT
    YEAR(o.order_purchase_timestamp) AS anio,
    MONTH(o.order_purchase_timestamp) AS mes,
    COUNT(*) AS pedidos,
    SUM(oi.price) AS total_ingresos,
    SUM(oi.freight_value) AS total_flete
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE order_status = 'delivered'
   OR (order_delivered_carrier_date IS NOT NULL
       AND order_delivered_customer_date IS NOT NULL)
GROUP BY YEAR(o.order_purchase_timestamp), MONTH(o.order_purchase_timestamp)
ORDER BY 1, 2;

-- 9. Categorías con clientes más insatisfechos (JOIN de 5 tablas)
SELECT
    c.product_category_name_english AS categoria_en,
    CAST(AVG(r.review_score * 1.0) AS DECIMAL(3,2)) AS avg_review_score,
    COUNT(*) AS reviews_totales,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS reviews_negativos,
    CAST(SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_reviews_negativos
FROM order_reviews r
INNER JOIN orders o ON r.order_id = o.order_id
INNER JOIN order_items i ON o.order_id = i.order_id
INNER JOIN products p ON i.product_id = p.product_id
FULL OUTER JOIN product_category_name_translation c ON p.product_category_name = c.product_category_name
GROUP BY c.product_category_name_english, c.product_category_name
HAVING COUNT(*) >= 50
ORDER BY AVG(r.review_score * 1.0);

-- 10. Top 10 estados por número de clientes (JOIN de 3 tablas)
SELECT TOP 10
    c.customer_state AS estado,
    COUNT(DISTINCT c.customer_unique_id) AS clientes_unicos,
    COUNT(DISTINCT o.order_id) AS ordenes_totales,
    SUM(p.payment_value) AS ingreso_total
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_payments p ON o.order_id = p.order_id
GROUP BY c.customer_state
ORDER BY COUNT(DISTINCT c.customer_unique_id) DESC;

-- =============================================
-- SECCIÓN 5: ANÁLISIS AVANZADO (WINDOW FUNCTIONS)
-- =============================================

-- 11. Ingreso acumulado Year-to-Date (SUM OVER PARTITION BY ORDER BY)
WITH ingreso_por_mes AS (
    SELECT
        YEAR(o.order_purchase_timestamp) AS anio,
        MONTH(o.order_purchase_timestamp) AS mes,
        SUM(oi.price) AS total_orden,
        SUM(oi.freight_value) AS total_flete,
        SUM(oi.price + oi.freight_value) AS total
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY YEAR(o.order_purchase_timestamp), MONTH(o.order_purchase_timestamp)
)
SELECT
    anio,
    mes,
    total_orden,
    SUM(total_orden) OVER(PARTITION BY anio ORDER BY mes) AS total_orden_acumulado,
    total_flete,
    SUM(total_flete) OVER(PARTITION BY anio ORDER BY mes) AS total_flete_acumulado,
    total,
    SUM(total) OVER(PARTITION BY anio ORDER BY mes) AS total_acumulado
FROM ingreso_por_mes;

-- 12. Artículo más caro comprado por cada cliente (DENSE_RANK)
WITH ranking_compras AS (
    SELECT
        o.customer_id AS cliente,
        oi.product_id AS producto_comprado,
        MAX(oi.price) AS precio,
        DENSE_RANK() OVER(PARTITION BY o.customer_id ORDER BY MAX(oi.price) DESC) AS ranking
    FROM orders o
    INNER JOIN order_items oi
    ON o.order_id = oi.order_id
    GROUP BY o.customer_id, oi.product_id
)
SELECT
    *
FROM ranking_compras
WHERE ranking = 1
ORDER BY precio DESC

-- 13. Crecimiento porcentual de ingresos mes a mes (LAG + CASE WHEN)
WITH ingresos_mensuales AS (
    SELECT
        YEAR(o.order_purchase_timestamp) AS anio,
        MONTH(o.order_purchase_timestamp) AS mes,
        SUM(oi.price + oi.freight_value) AS ingreso
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY YEAR(o.order_purchase_timestamp), MONTH(o.order_purchase_timestamp)
)
SELECT
    anio,
    mes,
    ingreso,
    CAST(
        (ingreso - LAG(ingreso) OVER(ORDER BY anio, mes)) * 100.0
        / LAG(ingreso) OVER(ORDER BY anio, mes)
    AS DECIMAL(12,2)) AS variacion_porcentual,
    CASE
        WHEN ingreso - LAG(ingreso) OVER(ORDER BY anio, mes) > 0 THEN 'incremento'
        WHEN ingreso - LAG(ingreso) OVER(ORDER BY anio, mes) = 0 THEN 'sin cambio'
        WHEN ingreso - LAG(ingreso) OVER(ORDER BY anio, mes) < 0 THEN 'decremento'
    END AS tendencia
FROM ingresos_mensuales
ORDER BY anio, mes;
