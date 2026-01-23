# Databricks notebook source
# MAGIC %run "/Workspace/Users/ganapathisking@gmail.com/ecommerce_pipeline/utils/utils_notebook"

# COMMAND ----------

from pyspark.sql.functions import sum, col  # Import libraries 
silver_delta_path = "dbfs:/Volumes/workspace/ecommerce/silver/events_delta"  # Path to store Silver Delta table
gold_delta_path = "dbfs:/Volumes/workspace/ecommerce/gold/events_delta"  # Path to store Gold Delta table
silver_df = spark.read.format("delta").load(silver_delta_path)  # Read Silver Delta table
# Business aggregate: total revenue per brand
gold_df = silver_df.groupBy("brand").agg(sum("price").alias("total_revenue")).orderBy(col("total_revenue").desc())
create_delta_layer_if_not_exists(gold_df, gold_delta_path, "Gold")  # Write Gold