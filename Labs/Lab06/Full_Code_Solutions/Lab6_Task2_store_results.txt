from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

json_schema = T.StructType([
    T.StructField('application_name', T.StringType()),
    T.StructField('num_of_positive_sentiments', T.LongType()),
    T.StructField('num_of_neutral_sentiments', T.LongType()),
    T.StructField('num_of_negative_sentiments', T.LongType()),
    T.StructField('avg_sentiment_polarity', T.DoubleType()),
    T.StructField('avg_sentiment_subjectivity', T.DoubleType()),
    T.StructField('category', T.StringType()),
    T.StructField('rating', T.StringType()),
    T.StructField('reviews', T.StringType()),
    T.StructField('size', T.StringType()),
    T.StructField('num_of_installs', T.DoubleType()),
    T.StructField('price', T.DoubleType()),
    T.StructField('age_limit', T.LongType()),
    T.StructField('genres', T.StringType()),
    T.StructField('version', T.StringType())])

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('ex6_store_results') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .getOrCreate()

stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "gps-with-reviews") \
    .option('startingOffsets', 'earliest') \
    .load() \
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))

query = parsed_df \
    .writeStream \
    .trigger(processingTime='1 minute') \
    .format('parquet') \
    .outputMode('append') \
    .option("path", "hdfs://course-hdfs:8020/data/target/google_reviews_calc") \
    .option('checkpointLocation', 'hdfs://course-hdfs:8020/checkpoints/ex6/store_result') \
    .start()

query.awaitTermination()

spark.stop()
