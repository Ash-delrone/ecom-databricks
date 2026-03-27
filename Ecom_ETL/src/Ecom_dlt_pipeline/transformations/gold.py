import dlt
from pyspark.sql.functions import *

# ── Gold 1: Revenue by category ───────────────────────────────────
@dlt.table(
    name="gold_revenue_by_category",
    comment="Total revenue by product category — delivered orders only"
)
def gold_revenue_by_category():
    orders      = spark.read.table("silver_orders")
    order_items = spark.read.table("silver_order_items")
    products    = spark.read.table("silver_products")

    return (
        order_items
        .join(orders, "order_id", "inner")
        .join(products, "product_id", "left")
        .filter(col("order_status") == "delivered")
        .withColumn("product_category_name",coalesce(col("product_category_name"), lit("uncategorized")))
        .groupBy("product_category_name")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("price"), 2).alias("total_revenue"),
            round(sum("freight_value"), 2).alias("total_freight")
        )
        .orderBy(col("total_revenue").desc())
    )


# ── Gold 2: Revenue by region ─────────────────────────────────────
@dlt.table(
    name="gold_revenue_by_region",
    comment="Total revenue by customer state — delivered orders only"
)
def gold_revenue_by_region():
    orders      = spark.read.table("silver_orders")
    order_items = spark.read.table("silver_order_items")
    customers   = spark.read.table("silver_customers")

    return (
        order_items
        .join(orders, "order_id", "inner")
        .join(customers, "customer_id", "inner")
        .filter(col("order_status") == "delivered")
        .groupBy("customer_state")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("price"), 2).alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )


# ── Gold 3: Monthly order trends ──────────────────────────────────
@dlt.table(
    name="gold_monthly_trends",
    comment="Monthly order volume and revenue over time"
)
def gold_monthly_trends():
    orders      = spark.read.table("silver_orders")
    order_items = spark.read.table("silver_order_items")

    return (
        order_items
        .join(orders, "order_id", "inner")
        .filter(col("order_status") == "delivered")
        .withColumn("order_year",year(col("order_purchase_timestamp")))
        .withColumn("order_month",month(col("order_purchase_timestamp")))
        .groupBy("order_year", "order_month")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("price"), 2).alias("total_revenue")
        )
        .orderBy("order_year", "order_month")
    )


# ── Gold 4: Payment method breakdown ──────────────────────────────
@dlt.table(
    name="gold_payment_breakdown",
    comment="Revenue and transaction count by payment type"
)
def gold_payment_breakdown():
    orders   = spark.read.table("silver_orders")
    payments = spark.read.table("silver_order_payments")

    return (
        payments
        .join(orders, "order_id", "inner")
        .filter(col("order_status") == "delivered")
        .groupBy("payment_type")
        .agg(
            count("order_id").alias("total_transactions"),
            round(sum("payment_value"), 2).alias("total_value"),
            round(sum("payment_value") / count("order_id"), 2)
                .alias("avg_transaction_value")
        )
        .orderBy(col("total_value").desc())
    )