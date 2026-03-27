import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_orders",
    comment="Raw orders from Olist CSV via Volumes — append only"
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