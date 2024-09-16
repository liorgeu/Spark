from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Create or retrieve an existing Spark session:
spark = SparkSession.builder.master("local").appName('ex2_flights').getOrCreate()

# Using the read.csv method, fetch the flight data from the given S3 path:
flights_raw_df = spark.read.csv('s3a://spark/data/raw/flights/', header=True)

flights_raw_df .show()

# write this transformed DataFrame back to S3 in Parquet format.

flights_df = flights_raw_df.select(
F.col('DayofMonth').cast(T.IntegerType()).alias('day_of_month'),
F.col('DayOfWeek').cast(T.IntegerType()).alias('day_of_week'),
F.col('Carrier').alias('carrier'),
F.col('OriginAirportID').cast(T.IntegerType()).alias('origin_airport_id'),
F.col('DestAirportID').cast(T.IntegerType()).alias('dest_airport_id'),
F.col('DepDelay').cast(T.IntegerType()).alias('dep_delay'),
F.col('ArrDelay').cast(T.IntegerType()).alias('arr_delay')
)

flights_df.show()

# Parquet is an efficient columnar storage format.
# The mode 'overwrite' is specified to replace the existing data if it exists.
flights_df.write.parquet('s3a://spark/data/source/flights/',mode='overwrite')


# terminate the Spark session to release its resources
# a good practice to clean up after your operations
spark.stop()

print("finished")