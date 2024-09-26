from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


enriched_event_schema = T.StructType([\
    T.StructField('event_id', T.StringType(), True),\
    T.StructField('event_time', T.TimestampType(), True),\
    T.StructField('car_id', T.IntegerType(), True),\
    T.StructField('driver_id', T.IntegerType(), True),\
    T.StructField('car_brand', T.StringType(), True),\
    T.StructField('car_model', T.StringType(), True),\
    T.StructField('color_name', T.StringType(), True),\
    T.StructField('speed', T.IntegerType(), True),\
    T.StructField('rpm', T.IntegerType(), True),\
    T.StructField('gear', T.IntegerType(), True),\
    T.StructField('expected_gear', T.IntegerType(), True),\
])


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('data_enrichment')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()    
    


streaming_df = spark.readStream \
    .format('kafka')\
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "samples-enriched") \
    .option('startingOffsets', 'latest') \
    .load()\
    .select(F.col('value').cast(T.StringType()))
    

parsed_df = streaming_df.withColumn('parsed_json', F.from_json(F.col('value'), enriched_event_schema))\
                    .select(F.col('parsed_json.*'))

filterd_df = parsed_df.filter((F.col('speed') > 120)\
                    | (F.col('gear') != F.col('expected_gear')) \
                    | (F.col('rpm') > 6000))


# # Print the data
# query = filterd_df.writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# Send to Alerts Kafka
query = filterd_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option('checkpointLocation', 's3a://spark/checkpoints/project/alert_data2') \
    .outputMode('append') \
    .start()

# wait until stream finishes
query.awaitTermination()       