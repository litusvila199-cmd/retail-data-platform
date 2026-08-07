from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("country", StringType(), False),
    StructField("city", StringType(), False),
    StructField("registration_date", DateType(), False),
    StructField("status", StringType(), False),
])


PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("created_at", DateType(), False),
])


ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), False),
])


ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
])


PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("payment_date", DateType(), False),
    StructField("payment_method", StringType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("status", StringType(), False),
])


SHIPMENTS_SCHEMA = StructType([
    StructField("shipment_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("shipment_date", DateType(), False),
    StructField("carrier", StringType(), False),
    StructField("tracking_number", StringType(), False),
    StructField("status", StringType(), False),
])

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "shipments": SHIPMENTS_SCHEMA,
}