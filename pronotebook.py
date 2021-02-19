# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://yelpcontainer@storageyelp.blob.core.windows.net",
mount_point = "/mnt/yelpcontainer",
extra_configs = {"fs.azure.account.key.storageyelp.blob.core.windows.net":dbutils.secrets.get(scope = "secretscope", key = "secret7")})

# COMMAND ----------

df = spark.read.json("/mnt/yelpcontainer/business_sample_cleveland.json")
df1=spark.read.json("/mnt/yelpcontainer/review_sample_cleveland.json")

# COMMAND ----------

df.show()

# COMMAND ----------

business = spark.read.format('json').load("/mnt/yelpcontainer/business_sample_cleveland.json")

# COMMAND ----------

business.write.mode("append").parquet("/mnt/yelpcontainer/business_sample")
display(dbutils.fs.ls("/mnt/yelpcontainer/business_sample"))

# COMMAND ----------

bus = spark.read.format('parquet').load("/mnt/yelpcontainer/business_sample")

# COMMAND ----------

display(bus)

# COMMAND ----------

df = spark.read.json("/mnt/yelpcontainer/business_sample_cleveland.json")

# COMMAND ----------

part=df.write.partitionBy("stars")

# COMMAND ----------

display(part)

# COMMAND ----------

parquetFile = spark.read.parquet("/mnt/yelpcontainer/business_sample")
parquetFile.createOrReplaceTempView("parquetFile")
i = spark.sql("SELECT categories,review_count FROM parquetFile ORDER BY review_count DESC LIMIT 10")
i.show()

# COMMAND ----------


