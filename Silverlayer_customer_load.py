# Databricks notebook source
spark.sql("USE globalretail_silver")
spark.sql("""
    Create table if not exists silver_customers(
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING, 
    total_purchases INT,
    customer_segment STRING,
    days_since_registration INT,
    last_updated TIMESTAMP)
""")
    
    

# COMMAND ----------

#Getting the last processed timestamp from silver layer
last_processed_df = spark.sql("Select MAX(last_updated) as last_processed from Silver_customers")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

#Create temproray view of incremental bronze data
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
SELECT *
FROM globalretail_bronze.bronze_customer c where c.ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

spark.sql("select * from bronze_incremental").show()

# COMMAND ----------

#Validate the email (null or not null)
#valid the ag ebetween 18-100
#Create customer_segment as total purchases > 1000 as 'High Value' if > 5000 as 'Medium' Else 'LOw Value'
#days since user is registered in the system
#Remove any junk records where total_purchase is negative number

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
SELECT 
    customer_id,
    name,
    email,
    country,
    customer_type,
    registration_date,
    age,
    gender,
    total_purchases,
    CASE
        WHEN total_purchases > 1000 THEN 'High Value'   
        WHEN total_purchases > 500 THEN 'Medium'
        ELSE 'Low Value'
    END AS customer_segment,
    datediff(current_date(), registration_date) AS days_since_registration, 
    current_timestamp() AS last_updated
FROM bronze_incremental
WHERE email IS NOT NULL
AND age BETWEEN 18 AND 100
""")
 

# COMMAND ----------

display(spark.sql("select * from silver_incremental"))

# COMMAND ----------

spark.sql("""
MERGE INTO silver_customers target
USING silver_incremental source
ON target.customer_id= source.customer_id
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN
INSERT *
""")

# COMMAND ----------

spark.sql("select * from silver_customers").show()