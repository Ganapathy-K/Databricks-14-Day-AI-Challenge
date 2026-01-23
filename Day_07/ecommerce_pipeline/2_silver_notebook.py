# Databricks notebook source
# MAGIC %run "/Workspace/Users/ganapathisking@gmail.com/ecommerce_pipeline/utils/utils_notebook"

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp  # Import libraries 
bronze_delta_path = "dbfs:/Volumes/workspace/ecommerce/bronze/events_delta"  # Path to store Bronze Delta table
silver_delta_path = "dbfs:/Volumes/workspace/ecommerce/silver/events_delta"  # Path to store Silver Delta table
bronze_df = spark.read.format("delta").load(bronze_delta_path)  # Read Bronze Delta table
silver_df = (
    bronze_df
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("price", col("price").cast("double"))
    .dropDuplicates()
    .filter(col("price").isNotNull())
)  # Clean and transform
create_delta_layer_if_not_exists(silver_df, silver_delta_path, "Silver")  # Write Silver
