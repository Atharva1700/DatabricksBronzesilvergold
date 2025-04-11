# Databricks notebook source
filepath="dbfs:/FileStore/Globalretail/Bronze_layer/product_catalog_data/products.json"
df=spark.read.json(filepath)
df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new=df.withColumn("ingestion_timestamp",current_timestamp())

display(df_new)

# COMMAND ----------

spark.sql("use globalretail_bronze")
df_new.write.format("delta").mode("append").saveAsTable("bronze_product_catlog")

# COMMAND ----------

spark.sql("select * from bronze_product_catlog limit 10").show()

# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/Globalretail/Bronze_layer/product_catalog_data/archive/"
archive_filepath=archive_folder + '_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
dbutils.fs.mv(filepath,archive_filepath)
print(archive_filepath)
