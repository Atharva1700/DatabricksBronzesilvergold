# Databricks notebook source
spark.sql("create database if not exists globalretail_bronze")

# COMMAND ----------

spark.sql("show Databases").show()

# COMMAND ----------

spark.sql("use globalretail_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select current_database()