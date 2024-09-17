from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName('ex3_aggregations').getOrCreate()

flights_df = spark.read.parquet('hdfs://course-hdfs:8020/data/transformed/flights/')
airports_df = spark.read.parquet('hdfs://course-hdfs:8020/data/source/airports/')

flights_df.printSchema()
airports_df.printSchema()

flights_df \
    .groupBy(F.col('origin_airport_id').alias('airport_id')) \
    .agg(F.count(F.lit(1)).alias('number_of_departures')) \
    .join(airports_df.select(F.col('airport_id'), F.col('name').alias('airport_name')), ['airport_id']) \
    .orderBy(F.col('number_of_departures').desc()) \
    .show(10, False)

flights_df \
    .groupBy(F.col('dest_airport_id').alias('airport_id')) \
    .agg(F.count(F.lit(1)).alias('number_of_arrivals')) \
    .join(airports_df.select(F.col('airport_id'), F.col('name').alias('airport_name')), ['airport_id']) \
    .orderBy(F.col('number_of_arrivals').desc()) \
    .show(10, False)

flights_df \
    .groupBy(F.col('origin_airport_id').alias('source_airport_id'), F.col('dest_airport_id').alias('dest_airport_id')) \
    .agg(F.count(F.lit(1)).alias('number_of_tracks')) \
    .join(airports_df.select(F.col('airport_id').alias('source_airport_id'),
                             F.col('name').alias('source_airport_name')),
          ['source_airport_id']) \
    .join(airports_df.select(F.col('airport_id').alias('dest_airport_id'),
                             F.col('name').alias('dest_airport_name')),
          ['dest_airport_id']) \
    .select(F.concat(F.col('source_airport_name'), F.lit(' -> '), F.col('dest_airport_name')).alias('track'),
            F.col('number_of_tracks')) \
    .orderBy(F.col('number_of_tracks').desc()) \
    .show(10, False)

# Complete the solution

spark.stop()
