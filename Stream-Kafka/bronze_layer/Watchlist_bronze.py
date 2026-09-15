import json
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame




@dp.table(
    name = "stream_layer.bronze.watchlist_bronze"
)

def watchlist_bronze() -> DataFrame:
    source_path='/Volumes/stream_layer/source/fraud_watchlist/source/'


    raw_data=(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
    )


    
    transformed_data = raw_data.select(
        "*",
        F.col("_metadata.file_path").alias("file_path"),
        F.current_timestamp().alias("ingest_time")
    )

    return transformed_data
