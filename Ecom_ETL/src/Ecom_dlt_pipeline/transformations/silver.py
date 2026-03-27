import dlt
from pyspark.sql.functions import *

@dlt.view(
    name="v_silver_orders",
    comment="Cleaned and typed orders — intermediate view before SCD merge"
)
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_purchase_timestamp", "order_purchase_timestamp IS NOT NULL")
@dlt.expect("valid_order_status","order_status IN ('delivered','shipped','canceled','processing','approved')")

def v_silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .withColumn("order_purchase_timestamp",to_timestamp(col("order_purchase_timestamp")))
        .withColumn("order_approved_at",to_timestamp(col("order_approved_at")))
        .withColumn("order_delivered_customer_date",to_timestamp(col("order_delivered_customer_date")))
        .withColumn("order_estimated_delivery_date",to_timestamp(col("order_estimated_delivery_date")))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .withColumn("_quality_flag",when(col("order_approved_at").isNull(), "missing_approval")
                                    .when(col("order_delivered_customer_date").isNull(), "not_delivered")
                                    .otherwise("ok"))
        .dropDuplicates(["order_id"])
    )


@dlt.view(name="v_silver_customers")
@dlt.expect_or_drop("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_state", "customer_state IS NOT NULL")

def v_silver_customers():
    return (
        spark.readStream.table("bronze_customers")
        .withColumn("customer_zip_code_prefix",col("customer_zip_code_prefix").cast("integer"))
        .withColumn("customer_state", trim(col("customer_state")))
        .dropDuplicates(["customer_id"])
    )


@dlt.view(name="v_silver_order_items")
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price > 0")
@dlt.expect("valid_freight", "freight_value >= 0")

def v_silver_order_items():
    return (
        spark.readStream.table("bronze_order_items")
        .withColumn("price", col("price").cast("double"))
        .withColumn("freight_value", col("freight_value").cast("double"))
        .withColumn("shipping_limit_date",to_timestamp(col("shipping_limit_date")))
        .dropDuplicates(["order_id", "order_item_id"])
    )


@dlt.view(name="v_silver_order_payments")
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_payment", "payment_value > 0")
@dlt.expect("valid_payment_type","payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')")

def v_silver_order_payments():
    return (
        spark.readStream.table("bronze_order_payments")
        .withColumn("payment_value", col("payment_value").cast("double"))
        .withColumn("payment_installments",col("payment_installments").cast("integer"))
        .dropDuplicates(["order_id", "payment_sequential"])
    )

@dlt.view(name = "v_silver_products")
@dlt.expect_or_drop("non_null_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("non_null_category", "product_category_name IS NOT NULL")

def v_silver_products():
    return (
        spark.readStream.table("bronze_products")
        .withColumn("product_category_name", trim(col("product_category_name")))
        .withColumn("product_name_lenght", col("product_name_lenght").cast("double"))
        .withColumn("product_photos_qty", col("product_photos_qty").cast("integer"))
        .withColumn("product_weight_g", col("product_weight_g").cast("double"))
        .withColumn("product_length_cm", col("product_length_cm").cast("double"))
        .withColumn("product_height_cm", col("product_height_cm").cast("double"))
        .withColumn("product_width_cm", col("product_width_cm").cast("double"))
        .dropDuplicates(["product_id"])
    )



dlt.create_streaming_table("silver_orders")
dlt.create_auto_cdc_flow(
    target = "silver_orders",
    source = "v_silver_orders",
    keys = ["order_id"],             
    sequence_by = "order_purchase_timestamp",  
    stored_as_scd_type = 1      
)

dlt.create_streaming_table("silver_customers")
dlt.create_auto_cdc_flow(
    target = "silver_customers",
    source = "v_silver_customers",
    keys = ["customer_id"],
    sequence_by = "_ingestion_time",
    stored_as_scd_type = 1
)

dlt.create_streaming_table("silver_order_items")
dlt.create_auto_cdc_flow(
    target = "silver_order_items",
    source = "v_silver_order_items",
    keys = ["order_id", "order_item_id"],
    sequence_by = "shipping_limit_date",
    stored_as_scd_type = 1
)

dlt.create_streaming_table("silver_order_payments")
dlt.create_auto_cdc_flow(
    target = "silver_order_payments",
    source = "v_silver_order_payments",
    keys = ["order_id", "payment_sequential"],
    sequence_by = "_ingestion_time",
    stored_as_scd_type = 1
)

dlt.create_streaming_table("silver_products")
dlt.create_auto_cdc_flow(
    target = "silver_products",
    source = "v_silver_products",
    keys = ["product_id"],
    sequence_by = "_ingestion_time",
    stored_as_scd_type = 1)









