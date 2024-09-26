from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType


#read Schema tables
model_schema = StructType([
    StructField("model_id", IntegerType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True)
])

color_schema = StructType([
    StructField("color_id", IntegerType(), True),
    StructField("color_name", StringType(), True)
])

car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

# Create a Spark session
spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('data_enrichment')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()    
    

# Read static tables
cars = spark.read\
    .schema(car_schema)\
    .parquet('s3a://spark/data/cars')
    
models = spark.read\
    .schema(model_schema)\
    .parquet('s3a://spark/data/car_models')
    
colors = spark.read\
    .schema(color_schema)\
    .parquet('s3a://spark/data/car_colors')


# readStream data from Kafka
streaming_df = spark.readStream \
    .format('kafka')\
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample") \
    .option('startingOffsets', 'latest') \
    .load()\
    .select(F.col('value').cast(T.StringType()))
    

parsed_df = streaming_df.withColumn('parsed_json', F.from_json(F.col('value'), event_schema)).select(F.col('parsed_json.*'))
    
joined_df = parsed_df.join(F.broadcast(cars), 'car_id')\
                    .join(F.broadcast(models), 'model_id')\
                    .join(F.broadcast(colors), 'color_id')\
                    .select('event_id', 'event_time', 'car_id', 'driver_id', 'car_brand', 'car_model',\
                        'color_name', 'speed', 'rpm', 'gear')
                    
enriched_df = joined_df.withColumn('expected_gear', F.ceil(F.col('speed')/ F.lit(30.0)).cast('int'))

# # Print the data
# query = joined_df.writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()


query = enriched_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option('checkpointLocation', 's3a://spark/checkpoints/project/samples-enriched2') \
    .outputMode('append') \
    .start()
    
# wait until stream finishes
query.awaitTermination()           
    
# Stop the Spark session
spark.stop()
