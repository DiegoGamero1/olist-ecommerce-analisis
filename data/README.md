# Datos del Proyecto

Los archivos CSV originales **no se incluyen en este repositorio** debido a su tamaño (~150 MB en total).

## Descarga del Dataset

**Brazilian E-Commerce Public Dataset by Olist**  
🔗 https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

### Archivos necesarios (9 CSVs):

| Archivo | Registros | Descripción |
|---------|-----------|-------------|
| `olist_customers_dataset.csv` | 99,441 | Clientes y su ubicación |
| `olist_orders_dataset.csv` | 99,441 | Órdenes con timestamps y status |
| `olist_order_items_dataset.csv` | 112,650 | Ítems vendidos por orden (precio + flete) |
| `olist_order_payments_dataset.csv` | 103,886 | Pagos por orden (método, cuotas, monto) |
| `olist_order_reviews_dataset.csv` | 99,224 | Reviews con score y comentarios |
| `olist_products_dataset.csv` | 32,951 | Productos y sus categorías |
| `olist_sellers_dataset.csv` | 3,095 | Vendedores y su ubicación |
| `olist_geolocation_dataset.csv` | 1,000,163 | Coordenadas por código postal |
| `product_category_name_translation.csv` | 71 | Traducción portugués → inglés de categorías |

### Instrucciones

1. Descarga el dataset desde el enlace de Kaggle
2. Extrae los CSVs en `C:\olist\olist_data\`
3. Ejecuta `sql/01_creacion_tablas_y_carga.sql` en SSMS

> **Nota:** Los scripts SQL esperan los archivos en `C:\olist\olist_data\`. Si usas otra ruta, actualiza las sentencias `BULK INSERT` del archivo `01_creacion_tablas_y_carga.sql`.
