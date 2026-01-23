# Databricks notebook source
# MAGIC %run "/Workspace/Users/ganapathisking@gmail.com/ecommerce_pipeline/utils/utils_notebook"

# COMMAND ----------

bronze_csv_path = "dbfs:/Volumes/workspace/ecommerce/bronze/events/2019-Nov.csv"  # Path to raw CSV
bronze_delta_path = "dbfs:/Volumes/workspace/ecommerce/bronze/events_delta"  # Path to store Bronze Delta table
bronze_df = spark.read.format("csv").option("header", "true").load(bronze_csv_path)  # Read raw CSV
create_delta_layer_if_not_exists(bronze_df, bronze_delta_path, "Bronze")  # Write Bronze
