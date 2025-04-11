# Databricks notebook source
spark.sql("USE globalretail_silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_products(
product_id STRING,
name STRING,
category STRING,
brand STRING,
price DOUBLE,
stock_quantity INT,
rating DOUBLE,
is_active BOOLEAN,
price_category STRING,
stock_status STRING,
last_updated TIMESTAMP
)
USING DELTA 
""")

# COMMAND ----------

# Replace 'your_table_name' with the actual table name
result = spark.sql("DESCRIBE silver_products")
result.show(truncate=False)


# COMMAND ----------

#Getting the last processed timestamp from silver layer
last_processed_df = spark.sql("Select MAX(last_updated) as last_processed from silver_products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_products AS
SELECT * 
FROM globalretail_bronze.bronze_product_catlog WHERE INGESTION_TIMESTAMP > '{last_processed_timestamp}'
""".format(last_processed_timestamp=last_processed_timestamp)          )

# COMMAND ----------


#Data transformation

#Price normalization(setting the negative price to 0)
#stock quantity normalization(setting the negative stock  to 0)
#Rating normalization (clamping between 0-5)
#price categorizing(premium standard budget)
#stock ststsus calculation(out of stock, low stock, moderate stock, sufficient stock)

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
SELECT 
    product_id,
    name,
    category,
    brand,
   CASE
      WHEN price < 0 THEN 0
      ELSE PRICE
END AS price,
CASE
   WHEN stock_quantity < 0 THEN 0
   ELSE stock_quantity
END AS stock_quantity,
CASE
   WHEN Rating < 0 THEN 0
   WHEN Rating > 5 THEN 5
   ELSE rating
End AS rating,
is_active,
CASE 
   WHEN price > 1000 THEN 'premium'
   WHEN price > 100 THEN 'standard'
   ELSE 'budget'
END AS price_category,
CASE
   WHEN stock_quantity = 0 THEN 'out of stock'
   WHEN stock_quantity < 10 THEN 'low stock'
   whEN stock_quantity < 50 THEN 'moderate stock'
   ELSE 'sufficient stock'
END AS stock_status,
    current_timestamp() As last_updated
FROM bronze_incremental_products
Where name is not NULL AND category is NOT NULL
""")

# COMMAND ----------

# Check schema of the view
spark.sql("DESCRIBE silver_incremental_products").show()


# COMMAND ----------

spark.sql("""
MERGE INTO silver_products target 
USING silver_incremental_products source
ON target.product_id = source.product_id
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN
INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalretail_silver.silver_products