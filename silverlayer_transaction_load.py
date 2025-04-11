# Databricks notebook source
spark.sql("USE globalretail_silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS silvers_order (
    transaction_id STRING,  
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    payment_method STRING,
    store_type STRING,
    order_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark,sql("select * from silvers_order").show()

# COMMAND ----------

#Get the last processsed timrstamp from silver layer
last_processed_df= spark.sql("SELECT MAX(last_updated) as last_processed FROM silvers_order")
last_processed_timestamp=last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp='1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,col
bronze_transactionsss = globalretail_bronze.bronze_transactions.withColumn("transaction_date",to_timestamp(col("transaction_date")))
globalretail_bronze.bronze_transactions.show()

# COMMAND ----------

#Create a temporary view of incremental bronze data 
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_orders AS 
SELECT * FROM 
globalretail_bronze.bronzes_transactions WHERE ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from  bronze_incremental_orders

# COMMAND ----------

#Data Transformation 
#-Quantity and total_amount normalization
#Date casting to ensure consistency date format
#Order status derivation based on quantity and total_amount 

#___Data Quality Checks: We filter out records with null transaction dates , csutomeridsor productid.

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_orders As 
SELECT 
    transaction_id,
    customer_id,
    product_id,
    CASE    
         WHEN quantity < 0 THEN 0
         ELSE quantity 
    END AS quantity,  
    CASE 
        WHEN total_amount < 0 THEN 0
        ELSE total_amount
    END AS total_amount,
    CAST(transaction_date AS DATE) AS transaction_date,
    payment_method,
    store_type,
    CASE 
        WHEN quantity = 0 AND total_amount = 0 THEN 'Cancelled'
        ELSE 'COMPLETED'
    END AS order_status,
    current_timestamp() AS last_updated
FROM bronze_incremental_orders
WHERE transaction_date IS NOT NULL
 and customer_id IS NOT NULL
 and product_id IS NOT NULL
 """) 


# COMMAND ----------

spark.sql("select * from silver_incremental_orders").show()

# COMMAND ----------

spark.sql("""
MERGE INTO silvers_order target
USING (
    SELECT 
        CAST(customer_id AS STRING) AS customer_id,
        CAST(product_id AS STRING) AS product_id,
        transaction_id,
        quantity,
        total_amount,
        transaction_date,
        payment_method,
        store_type,
        order_status,
        last_updated
    FROM silver_incremental_orders
) source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN 
  UPDATE SET 
    target.customer_id = source.customer_id,
    target.product_id = source.product_id,
    target.quantity = source.quantity,
    target.total_amount = source.total_amount,
    target.transaction_date = source.transaction_date,
    target.payment_method = source.payment_method,
    target.store_type = source.store_type,
    target.order_status = source.order_status,
    target.last_updated = source.last_updated
WHEN NOT MATCHED THEN 
  INSERT (
    transaction_id, 
    customer_id, 
    product_id, 
    quantity, 
    total_amount, 
    transaction_date, 
    payment_method, 
    store_type, 
    order_status, 
    last_updated
  ) 
  VALUES (
    source.transaction_id, 
    source.customer_id, 
    source.product_id, 
    source.quantity, 
    source.total_amount, 
    source.transaction_date, 
    source.payment_method, 
    source.store_type, 
    source.order_status, 
    source.last_updated
  )
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from silvers_order