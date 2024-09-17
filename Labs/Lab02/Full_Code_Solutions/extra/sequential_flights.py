from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_routing_paths') \
    .getOrCreate()

flights_df = spark.read.parquet('hdfs://course-hdfs:8020/data/transformed/flights/')

flights_df_distinct = flights_df.select(F.col('carrier'), \
                                        F.col('origin_airport_id'), \
                                        F.col('dest_airport_id'), \
                                        F.col('flight_date')) \
    .dropDuplicates()

window = Window \
    .partitionBy(F.col('carrier'), F.col('origin_airport_id'), F.col('dest_airport_id')) \
    .orderBy(F.col('flight_date'))

continuous_df = flights_df_distinct \
    .withColumn('last_flight', F.lag(F.col('flight_date')).over(window)) \
    .withColumn('next_flight', F.lead(F.col('flight_date')).over(window)) \
    .withColumn('is_first', F.when(F.isnull(F.col('last_flight')), F.lit(True))
                .otherwise(~(F.col('flight_date') == F.date_add(F.col('last_flight'), 1)))) \
    .withColumn('is_last', F.when(F.isnull(F.col('next_flight')), F.lit(True))
                .otherwise(~(F.col('flight_date') == F.date_add(F.col('next_flight'), -1))))

ranges_df = continuous_df.where(F.col('is_first') | F.col('is_last'))

start_end_ranges_df = ranges_df \
    .select(F.col('carrier'),
            F.col('origin_airport_id'),
            F.col('dest_airport_id'),
            F.col('flight_date'),
            F.struct(F.col('flight_date').alias('flight_date'),
                     F.col('is_first').alias('is_first'),
                     F.col('is_last').alias('is_last')).alias('flight_details')) \
    .withColumn('next_flight_details', F.lead(F.col('flight_details')).over(window)) \
    .select(F.col('carrier'),
            F.col('origin_airport_id'),
            F.col('dest_airport_id'),
            F.when(F.col('flight_details.is_first'), F.col('flight_details.flight_date')).alias('start_range'),
            F.when(F.col('flight_details.is_last'), F.col('flight_details.flight_date'))
            .otherwise(F.when(F.col('next_flight_details.is_last'),
                              F.col('next_flight_details.flight_date'))).alias('end_range'))

min_max_df = start_end_ranges_df.where(F.col('start_range').isNotNull()) \
    .withColumn('num_of_flights', F.datediff(F.col('end_range'), F.col('start_range')) + 1)

min_max_df.orderBy(F.col('num_of_flights').desc()).show()

spark.stop()
