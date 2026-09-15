import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *


@dp.table(
    name="stream_layer.gold.fraud_transactions"
)

def fraud_transaction() -> DataFrame:
    transactions = spark.readStream.table("stream_layer.silver.kafka_transactions_silver")
    watchlists = spark.readStream.table("stream_layer.silver.watchlist_silver")
    customers = spark.read.table("stream_layer.silver.customers_silver")



    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")
    watchlists_with_watermark = watchlists.withWatermark("effective_from", "5 minutes")


    fraud_transactions = transactions_with_watermark.join(
        watchlists_with_watermark,
        transactions_with_watermark.card_number == watchlists_with_watermark.entity_id,
        "inner"
    ).join(
        customers,
        transactions_with_watermark.customer_id == customers.customer_id,
        "left"
    )
    
    fraud_df = fraud_transactions.select(
        F.concat(F.lit("ALERT-"), F.col("watchlist_id")).alias("alert_id"),
        F.lit("FRAUD-TRANSACTION").alias("alert_type"),
        F.current_timestamp().alias("Fraud_detected_at"),
        F.col("transaction_id"),
        transactions.card_number.alias("card_number"),
        F.col("amount"),
        F.col("transaction_timestamp"),
        customers.customer_id.alias("customer_id"),
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("customer_name"),
        F.col("email").alias("customer_email"),
        customers.city.alias("customer_city"),
        F.col("reason_code").alias("watchlist_reason")
    )

    return fraud_df
