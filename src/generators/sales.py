from pyspark.sql.functions import col
from spark.session import spark

# ============================================================
# 1. LEER LOS DATOS PROCESADOS
# ============================================================
orders = spark.read.parquet(
    "data/processed/orders.parquet"
)

order_items = spark.read.parquet(
    "data/processed/order_items.parquet"
)

products = spark.read.parquet(
    "data/processed/products.parquet"
)

# ============================================================
# 2. UNIR ORDERS CON ORDER_ITEMS
# ============================================================

sales = (
    orders
    .join(
        order_items,
        orders.order_id == order_items.order_id,
        "inner"
    )
    .select(
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        order_items.product_id,
        order_items.quantity,
        order_items.unit_price
    )
)

# ============================================================
# 3. UNIR EL RESULTADO CON PRODUCTS
# ============================================================

sales = (
    sales
    .join(
        products,
        sales.product_id == products.product_id,
        "inner"
    )
    .select(
        sales.order_id,
        sales.customer_id,
        sales.order_date,
        products.product_id,
        products.product_name,
        products.category,
        sales.quantity,
        sales.unit_price
    )
)

# ============================================================
# 4. CALCULAR EL TOTAL DE CADA LÍNEA DE VENTA
# ============================================================

sales = sales.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)


# ============================================================
# 5. GUARDAR EL DATASET CURATED
# ============================================================

sales.write.mode("overwrite").parquet(
    "data/curated/sales.parquet"
)

