# Databricks notebook source
spark.sql("USE globalretail_gold")
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_daily_saless AS
SELECT
    transaction_date,
    SUM(total_amount) as daily_total_sales
FROM 
    globalretail_silver.silvers_order
GROUP BY 
     transaction_date
     """)


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold_daily_saless