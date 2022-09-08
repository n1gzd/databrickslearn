# Databricks notebook source
blob_account_name = 'n1gzddb203learndl'
blob_container_name = 'youtube'
blob_relative_path = '/Tabledata.csv'
blob_sas_token = 'sp=rc&st=2022-09-08T00:10:58Z&se=2022-09-11T08:10:58Z&spr=https&sv=2021-06-08&sr=c&sig=fb25vMnwNLsYydMWMTXsOwS05jlQkLjzrivoSet%2F1c0%3D'

# COMMAND ----------

wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# COMMAND ----------

from pyspark.sql.types import *
user_schema = StructType([ \
    StructField("date", StringType(), True),\
    StructField("videos_added", IntegerType(), True),\
    StructField("average_views_per_viewer", IntegerType(), True),\
    StructField("subscribers", IntegerType(), True),\
    StructField("unique_viewers", IntegerType(), True),\
    StructField("views", IntegerType(), True),\
    StructField("watch_time_hours", IntegerType(), True),\
    StructField("average_duration", IntegerType(), True),])
df = spark.read.schema(user_schema).option("header","true")\
    .csv(wasbs_path)
df.createOrReplaceTempView('source')

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM source LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM source where videos_added>'7'
