from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

json_schema = T.StructType([
    T.StructField('application_name', T.StringType()),
    T.StructField('translated_review', T.StringType()),
    T.StructField('sentiment_rank', T.IntegerType()),
    T.StructField('sentiment_polarity', T.FloatType()),
    T.StructField('sentiment_subjectivity', T.FloatType())])

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('ex6_calculate_reviews') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "gps-user-review-source") \
    .option('startingOffsets', 'earliest') \
    .load() \
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))

static_data_df = spark.read.parquet('hdfs://course-hdfs:8020/data/source/google_apps/')

static_data_df.cache()

joined_df = parsed_df \
    .groupBy(F.col('application_name')) \
    .agg(F.sum(F.when(F.col('sentiment_rank') == 1, 1).otherwise(0)).alias('num_of_positive_sentiments'),
         F.sum(F.when(F.col('sentiment_rank') == 0, 1).otherwise(0)).alias('num_of_neutral_sentiments'),
         F.sum(F.when(F.col('sentiment_rank') == -1, 1).otherwise(0)).alias('num_of_negative_sentiments'),
         F.avg(F.col('sentiment_polarity')).alias('avg_sentiment_polarity'),
         F.avg(F.col('sentiment_subjectivity')).alias('avg_sentiment_subjectivity')) \
    .join(static_data_df, ['application_name'])

fields_list = joined_df.schema.fieldNames()
fields_as_cols = list(map(lambda col_name: F.col(col_name), fields_list))

json_df = joined_df \
    .withColumn('to_json_struct', F.struct(fields_as_cols)) \
    .select(F.to_json(F.col('to_json_struct')).alias('value'))

query = json_df \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "gps-with-reviews") \
    .option('checkpointLocation', 'hdfs://course-hdfs:8020/checkpoints/ex6/review_calculation') \
    .outputMode('update') \
    .start()

query.awaitTermination()

static_data_df.unpersist()

spark.stop()