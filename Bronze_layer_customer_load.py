# Databricks notebook source
filepath="dbfs:/FileStore/Globalretail/Bronze_layer/customer_data/customer.csv"
df=spark.read.csv(filepath,header=True,inferSchema=True)
df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new=df.withColumn("ingestion_timestamp",current_timestamp())

display(df_new)

# COMMAND ----------

spark.sql("use globalretail_bronze")
df_new.write.format("delta").mode("append").saveAsTable("bronze_customer")

# COMMAND ----------

spark.sql("select * from bronze_customer limit 10").show()

# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/Globalretail/Bronze_layer/customer_data/archive/"
archive_filepath=archive_folder + '_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
dbutils.fs.mv(filepath,archive_filepath)
print(archive_filepath)
