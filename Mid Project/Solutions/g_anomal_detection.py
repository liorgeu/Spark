from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def for_each_data(df,batch_id):
    # Calculate lagged values 
    df = df \
        .withColumn('last_avg_gear', F.lag(F.col('avg_gear')).over(Window.partitionBy("car_id").orderBy("start"))) \
        .withColumn('last_avg_speed', F.lag(F.col('avg_speed')).over(Window.partitionBy("car_id").orderBy("start"))) \
        .withColumn('last_avg_rpm', F.lag(F.col('avg_rpm')).over(Window.partitionBy("car_id").orderBy("start")))
            # .withColumn('rank', F.row_number().over(Window.partitionBy("car_id").orderBy(F.col("start").desc())))\

    # anomynal detecation  
    df = df\
        .where(F.col('avg_gear') - F.col('last_avg_gear') >= 1.0) \
        .where((F.col('avg_speed')/F.col('last_avg_speed'))-1 >= 0.1) \
        .where(F.col('avg_speed')-F.col('last_avg_speed') >= 30) \
        .where((F.col('avg_rpm')/F.col('last_avg_rpm'))-1 >= 0.2 ) \
        .where(F.col('avg_rpm')-F.col('last_avg_rpm') >= 500)           
                                
    # df.show()
    
    #write to kafka
    df\
        .withColumn('to_json_struct', F.struct('*')) \
        .select(F.to_json(F.col('to_json_struct')).alias('value')) \
        .write \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("topic", "anomaly-alerts")
    


enriched_event_schema = T.StructType([\
    T.StructField('event_id', T.FloatType(), True),\
    T.StructField('event_time', T.TimestampType(), True),\
    T.StructField('car_id', T.IntegerType(), True),\
    T.StructField('driver_id', T.IntegerType(), True),\
    T.StructField('car_brand', T.StringType(), True),\
    T.StructField('car_model', T.StringType(), True),\
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


# Apply the window specification along with the aggregation function
df_with_calculated_columns = parsed_df\
                            .withWatermark('event_time', '10 seconds')\
                            .groupBy(F.col('car_id'),F.window(F.col('event_time'), '10 seconds', '1 second').alias('event_time_data'))\
                            .agg(F.avg(F.col('gear')).alias('avg_gear'),\
                                F.avg(F.col('speed')).alias('avg_speed'),\
                                F.avg(F.col('rpm')).alias('avg_rpm')
                                )

df_with_calculated_columns = df_with_calculated_columns.select(F.col('*'),F.col('event_time_data.*'))

query = df_with_calculated_columns\
    .writeStream\
    .foreachBatch(for_each_data)\
    .option('checkpointLocation', 's3a://spark/checkpoints/project/alert_data3')\
    .start()

query.awaitTermination()