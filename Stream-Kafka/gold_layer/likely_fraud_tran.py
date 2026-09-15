import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *


@dp.table(
    name="stream_layer.gold.likely_fraud_transactions"
)

def likely_fraud_transactions() -> DataFrame:
    transactions = spark.readStream.table("stream_layer.silver.kafka_transactions_silver")
    customers = spark.read.table("stream_layer.silver.customers_silver")

    joined_df = (transactions.join(customers, transactions.customer_id == customers.customer_id, "left")
                    .filter(F.col("amount") > F.col("transaction_limit"))
                    .select(
                        F.concat_ws("-", F.lit("ALERT"), F.col("transaction_id")).alias("alart_id"),
                        F.lit("High Value Transaction").alias("alart_type"),
                        F.current_timestamp().alias("timestamp"),
                        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
                        transactions.transaction_id.alias("transaction_id"),
                        transactions.customer_id.alias("customer_id"),
                        customers.email.alias("customer_email"),
                        transactions.card_number.alias("customer_card_no"),
                        customers.annual_income.alias("customer_annual_income"),
                        transactions.merchant_id.alias("merchant_id"),
                        transactions.amount.alias("high_amount"),
                        transactions.currency.alias("currency"),
                        transactions.transaction_type.alias("transaction_type"),
                        transactions.country.alias("country")
                    ))

    return joined_df
