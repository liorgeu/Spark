from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex3_clean_flights') \
    .getOrCreate()

flights_df = spark.read.parquet('hdfs://course-hdfs:8020/data/source/flights/')
flights_raw_df = spark.read.parquet('hdfs://course-hdfs:8020/data/source/flights_raw/')

flights_distinct_df = flights_df.dropDuplicates()
flights_raw_distinct_df = flights_raw_df.dropDuplicates()

matched_df = flights_distinct_df.intersect(flights_raw_df)

unmatched_flights = flights_distinct_df.subtract(matched_df) \
    .withColumn('source_of_data', F.lit('flights'))

unmatched_flights_raw = flights_raw_distinct_df.subtract(matched_df) \
    .withColumn('source_of_data', F.lit('flights_raw'))

unmatched_df = unmatched_flights.union(unmatched_flights_raw)

matched_df.write.parquet('hdfs://course-hdfs:8020/data/stg/flight_matched/', mode='overwrite')
unmatched_df.write.parquet('hdfs://course-hdfs:8020/data/stg/flight_unmatched/', mode='overwrite')

spark.stop()
