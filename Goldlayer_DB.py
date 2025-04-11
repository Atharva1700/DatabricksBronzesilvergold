# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS globalretail_gold")
spark.sql("USE globalretail_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()