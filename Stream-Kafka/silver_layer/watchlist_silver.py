import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *



@dp.table(
    name = "stream_layer.silver.watchlist_silver",
    comment = "parsed and clean data watchlist data to silver"
)
def watchlist_transformed_data() -> DataFrame:
    watchlist_bronze_df = spark.readStream.table("stream_layer.bronze.watchlist_bronze")

    transformed_data = watchlist_bronze_df.select(
        F.col("watchlist_id"),
        F.to_timestamp(F.col("effective_from"),"dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.col("reason_description"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("reason_code"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("status"),
        F.col("watch_type"),
        F.col("city"),
        F.col("country"),
        F.col("action"),
        F.col("file_path"),
        F.col("ingest_time"),
        F.current_timestamp().alias("watchlist_silver_ingest_time")
    )


    return transformed_data
        
        