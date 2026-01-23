# Databricks notebook source
from delta.tables import DeltaTable
def create_delta_layer_if_not_exists(df, delta_path, layer_name):
    if DeltaTable.isDeltaTable(spark, delta_path):
        print(f"⚠️ {layer_name} layer already exists at {delta_path}")  # Check if Delta table already exists
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)  # Write as Delta table if it doesn't exist yet
        print(f"✅ {layer_name} layer created successfully at {delta_path}")
