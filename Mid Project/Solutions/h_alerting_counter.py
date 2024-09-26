from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("local") \
    .appName('alerting_counter') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Define the schema for the Kafka input stream
input_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("color_name", StringType(), True),
])

# Read from the enriched Kafka topic
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'course-kafka:9092') \
    .option('subscribe', 'alert-data') \
    .option('startingOffsets', 'latest') \
    .load() \
    .select(F.col('value').cast(StringType())) 

df = df \
    .withColumn('parsed_json', F.from_json(F.col('value'), input_schema)) \
    .select(F.col('parsed_json.*'))

df = df\
        .agg(F.count(F.col('event_id')).alias('num_of_rows'),
         F.sum(F.when(F.col('color_name') == "White", 1).otherwise(0)).alias('num_of_white'),
         F.sum(F.when(F.col('color_name') == "Sliver", 1).otherwise(0)).alias('num_of_silver'),
         F.sum(F.when(F.col('color_name') == "Black", 1).otherwise(0)).alias('num_of_black'),
         F.max(F.col('speed')).alias('max_speed'),
         F.max(F.col('rpm')).alias('max_rpm'),
         F.max(F.col('gear')).alias('max_gear'))


query = df.writeStream \
    .trigger(processingTime='1 minute') \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()