from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_anomalies_detection') \
    .getOrCreate()

all_history_window = Window.partitionBy(F.col('Carrier')).orderBy(F.col('flight_date'))
unbounded_window = Window.partitionBy(F.col('Carrier')).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
sliding_range_window = Window.partitionBy(F.col('Carrier')).orderBy(F.col('start_range'))

flights_df = spark.read.parquet('hdfs://course-hdfs:8020/data/transformed/flights/')

flights_df.cache()

flights_df \
    .withColumn('avg_till_now', F.avg(F.col('arr_delay')).over(all_history_window)) \
    .withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_till_now'))) \
    .where(F.col('avg_diff_percent') > F.lit(3.0)) \
    .show()

flights_df \
    .withColumn('avg_all_time', F.avg(F.col('arr_delay')).over(unbounded_window)) \
    .withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_all_time'))) \
    .where(F.col('avg_diff_percent') > F.lit(5.0)) \
    .show()

flights_df \
    .groupBy(F.col('Carrier'), F.window(F.col('flight_date'), '10 days', '1 day').alias('date_window')) \
    .agg(F.sum(F.col('dep_delay') + F.col('arr_delay')).alias('total_delay')) \
    .select(F.col('Carrier'),
            F.col('date_window.start').alias('start_range'),
            F.col('date_window.end').alias('end_range'),
            F.col('total_delay')) \
    .withColumn('last_window_delay', F.lag(F.col('total_delay')).over(sliding_range_window)) \
    .withColumn('change_percent', F.abs(F.lit(1.0) - (F.col('total_delay') / F.col('last_window_delay')))) \
    .where(F.col('change_percent') > F.lit(0.3)) \
    .show(100)

flights_df.unpersist()

spark.stop()
