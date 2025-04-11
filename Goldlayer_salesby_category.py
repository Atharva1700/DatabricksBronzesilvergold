# Databricks notebook source
spark.sql("USE globalretail_gold")
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_category_sales AS
SELECT p.category,
SUM(o.total_amount) as total_sales
FROM
globalretail_silver.silvers_order o 
JOIN
globalretail_silver.silver_products p on o.product_id=p.product_id
GROUP BY category 

""")



# COMMAND ----------

spark.sql("SELECT * FROM gold_category_sales").show()