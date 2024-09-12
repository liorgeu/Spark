from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]").appName('ex2_airports').getOrCreate()

airports_raw_df = spark.read.csv('hdfs://course-hdfs:8020/data/raw/airports/', header=True)

airports_df = airports_raw_df.select(
    F.col('airport_id').cast(T.IntegerType()).alias('airport_id'),
    F.col('city'),
    F.col('state'),
    F.col('name'))

airports_df.write.parquet('hdfs://course-hdfs:8020/data/source/airports/', mode='overwrite')

spark.stop()
