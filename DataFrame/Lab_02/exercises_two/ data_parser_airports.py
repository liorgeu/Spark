# initiate a Spark session to use Spark SQL's DataFrame API.
# Spark Session is a combined entry point of Spark Context and SQL Context
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark=SparkSession.builder.master("local").appName('ex2_airports').getOrCreate()

# Utilize Spark's read.csv method to read the CSV files from the S3 location.
#specify that the CSV has a header, so the first row will be used as column names.
airports_raw_df = spark.read.csv('s3a://spark/data/raw/airports/', header=True)
airports_raw_df.show()

# transform the data by selecting only the columns you need and casting the 'airport_id'
# column to IntegerType.
#alias it to keep the name consistent.
airports_df = airports_raw_df.select(
F.col('airport_id').cast(T.IntegerType()).alias('airport_id'),
F.col('city'),
F.col('state'),
F.col('name'))

airports_df.show()

# write the transformed DataFrame back to S3 in Parquet format.
airports_df.write.parquet('s3a://spark/data/source/airports/', mode='overwrite')

# print ("lior")

# terminate the Spark session to release its resources
# a good practice to clean up after your operations
spark.stop()


