import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *



@dp.table(
    name = "stream_layer.silver.kafka_transactions_silver",
    comment = "parsed and clean data"
)
@dp.expect_or_drop("valid_status", "status IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_transaction_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect_or_drop("valid_merchant_name", "merchant_name IS NOT NULL")
@dp.expect_or_drop("valid_merchant_category", "merchant_category IS NOT NULL")
@dp.expect_or_drop("valid_currency", "currency IS NOT NULL")
@dp.expect_or_drop("valid_payment_channel", "payment_channel IS NOT NULL")
@dp.expect_or_drop("valid_device_id", "device_id IS NOT NULL")
def transform_silver() -> DataFrame:
    broze_df = spark.readStream.table("stream_layer.bronze.kafka_transactions")

    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType())
    ])

    transformed_df = broze_df.select(
        F.from_json(F.col("value"), schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("event_timestamp").alias("timestamp"),
        F.col("ingesttime_bronze").alias("ingestion_timestamp_bronze")
    ).select(
        F.col("data.*"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("timestamp"),
        F.col("ingestion_timestamp_bronze"),
        F.current_timestamp().alias("ingestion_timestamp_silver")
    )

    return transformed_df

