# Databricks notebook source
filepath="dbfs:/FileStore/Globalretail/Bronze_layer/transaction/archive/transaction_snappy.parquet"
df=spark.read.parquet(filepath)
df.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,col
new_df= df.withColumn("transaction_date",to_timestamp(col("transaction_date")))
new_df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df=new_df.withColumn("ingestion_timestamp",current_timestamp())

display(final_df)

# COMMAND ----------

spark.sql("use globalretail_bronze")
final_df.write.format("delta").mode("append").saveAsTable("bronzes_transactions")

# COMMAND ----------

spark.sql("select * from bronzes_transactions").show()

# COMMAND ----------

bronze_transactions = spark.read.table("globalretail_bronze.bronze_transaction")
bronze_transactions.show(10)  # Check if data is loaded correctly


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# Convert 'transaction_date' to timestamp format
bronze_transactionsss = bronze_transactions.withColumn(
    "transaction_date", to_timestamp(col("transaction_date"))
)

bronze_transactionsss.show(10)  # Check the transformed DataFrame


# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/Globalretail/Bronze_layer/transaction/archive/"
archive_filepath=archive_folder + '_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
dbutils.fs.mv(filepath,archive_filepath)
print(archive_filepath)
