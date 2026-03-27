import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType())
])

dlt.create_streaming_table(
    name="bronze_orders_kafka",
    comment="Raw orders from Confluent Kafka topic Ecom_orders and historical data",
)

@dlt.append_flow(
    target = "bronze_orders_kafka",
    name="bronze_orders_kafka_streaming",
    comment="Real-time orders from Confluent Kafka topic Ecom_orders"
)
def bronze_orders_kafka_streaming():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",dbutils.secrets.get("kafka-secrets", "kafka-bootstrap-server"))
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
            'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="{}" password="{}";'.format(
                dbutils.secrets.get("kafka-secrets", "kafka-api-key"),
                dbutils.secrets.get("kafka-secrets", "kafka-api-secret")
            ))
        .option("subscribe", "Ecom_orders")
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("parsed",from_json(col("value").cast("string"), ORDER_SCHEMA))
        .select(
            "parsed.order_id",
            "parsed.customer_id",
            "parsed.order_status",
            "parsed.order_purchase_timestamp",
            "parsed.order_approved_at",
            "parsed.order_delivered_carrier_date",
            "parsed.order_delivered_customer_date",
            "parsed.order_estimated_delivery_date",
            col("timestamp").alias("_kafka_timestamp"),
            col("offset").alias("_kafka_offset")
        )
        .withColumn("_ingestion_time", current_timestamp())
    )



@dlt.append_flow(
    target = "bronze_orders_kafka",
    name="bronze_orders",
    comment="Raw orders from Olist CSV via Volumes — append only",
    # once = True
)
def bronze_orders():
    return (
        spark.readStream\
        .format("cloudFiles")\
        .option('cloudFiles.format', 'csv')\
        .option("header", "true")\
        .option("inferSchema", "false")\
        .option('pathGlobFilter', "*orders_dataset.csv")\
        .load('/Volumes/ecom/bronze/raw_data/')\
        .withColumn("_ingestion_time", current_timestamp())\
        .withColumn("_source_file", lit('olist_orders_dataset.csv'))
    )

@dlt.table(
    name="bronze_customers",
    comment="Raw customers from Olist CSV via Volumes"
)
def bronze_customers():
    return (
        spark.readStream\
        .format("cloudFiles")\
        .option('cloudFiles.format', 'csv')\
        .option("header", "true")\
        .option("inferSchema", "false")\
        .option('pathGlobFilter', "*customers_dataset.csv")\
        .load("/Volumes/ecom/bronze/raw_data/")\
        .withColumn("_ingestion_time", current_timestamp())\
        .withColumn("_source_file", lit('olist_customers_dataset.csv'))
    )

@dlt.table(
    name="bronze_order_items",
    comment="Raw order items from Olist CSV via Volumes"
)
def bronze_order_items():
    return (
        spark.readStream\
        .format("cloudFiles")\
        .option('cloudFiles.format', 'csv')\
        .option("header", "true")\
        .option("inferSchema", "false")\
        .option('pathGlobFilter', "*order_items_dataset.csv")\
        .load("/Volumes/ecom/bronze/raw_data/")\
        .withColumn("_ingestion_time", current_timestamp())\
        .withColumn("_source_file", lit('olist_order_items_dataset.csv'))
    )

@dlt.table(
    name="bronze_order_payments",
    comment="Raw payments from Olist CSV via Volumes"
)
def bronze_order_payments():
    return (
        spark.readStream\
        .format("cloudFiles")\
        .option('cloudFiles.format', 'csv')\
        .option("header", "true")\
        .option("inferSchema", "false")\
        .option('pathGlobFilter', "*order_payments_dataset.csv")\
        .load("/Volumes/ecom/bronze/raw_data/")\
        .withColumn("_ingestion_time", current_timestamp())\
        .withColumn("_source_file", lit('olist_order_payments_dataset.csv'))
    )

@dlt.table(
    name="bronze_products",
    comment="Raw products from Olist CSV via Volumes"
)
def bronze_products():
    return (
        spark.readStream\
        .format("cloudFiles")\
        .option('cloudFiles.format', 'csv')\
        .option("header", "true")\
        .option("inferSchema", "false")\
        .option('pathGlobFilter', "*products_dataset.csv")\
        .load("/Volumes/ecom/bronze/raw_data/")\
        .withColumn("_ingestion_time", current_timestamp())\
        .withColumn("_source_file", lit('olist_products_dataset.csv'))
    )