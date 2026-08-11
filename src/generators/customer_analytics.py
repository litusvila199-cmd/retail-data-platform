from pyspark.sql.functions import count, sum, max

from spark.session import spark


def generate_customer_analytics():

    # ============================================================
    # 1. LEER LOS DATOS
    # ============================================================

    customers = spark.read.parquet(
        "data/processed/customers.parquet"
    )

    orders = spark.read.parquet(
        "data/processed/orders.parquet"
    )

    sales = spark.read.parquet(
        "data/curated/sales.parquet"
    )

    # ============================================================
    # 2. UNIR CUSTOMERS CON ORDERS
    # ============================================================

    customer_orders = (
        customers
        .join(
            orders,
            customers.customer_id == orders.customer_id,
            "left"
        )
    )

    # ============================================================
    # 3. CALCULAR MÉTRICAS DE PEDIDOS POR CLIENTE
    # ============================================================

    customer_analytics = (
        customer_orders
        .groupBy(
            customers.customer_id,
            customers.first_name,
            customers.last_name
        )
        .agg(
            count(orders.order_id).alias("number_of_orders"),
            max(orders.order_date).alias("last_order_day")
        )
    )

    # ============================================================
    # 4. CALCULAR EL GASTO TOTAL POR CLIENTE
    # ============================================================

    customer_spending = (
        sales
        .groupBy("customer_id")
        .agg(
            sum("total_amount").alias("total_spent")
        )
    )

    # ============================================================
    # 5. AÑADIR EL GASTO TOTAL
    # ============================================================

    customer_analytics = (
        customer_analytics
        .join(
            customer_spending,
            customer_analytics.customer_id == customer_spending.customer_id,
            "left"
        )
        .select(
            customer_analytics.customer_id,
            customer_analytics.first_name,
            customer_analytics.last_name,
            customer_analytics.number_of_orders,
            customer_analytics.last_order_day,
            customer_spending.total_spent
        )
    )

    # ============================================================
    # 6. GUARDAR EL DATASET CURATED
    # ============================================================

    customer_analytics.write.mode("overwrite").parquet(
        "data/curated/customer_analytics.parquet"
    )
