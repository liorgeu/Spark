from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *


#================== connection between  spark and kafka=======================================#
#==============================================================================================
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('ROUTES_TO_S3') \
    .config('spark.jars', '/opt/drivers/postgresql-42.5.6.jar') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

#==============================================================================================
#=========================================== ReadStream from kafka===========================#
socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("Subscribe", "my_trip")\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#==============================================================================================
#==============================Create schema for create df from json=========================#
schema = t.StructType() \
    .add("vendorid", t.StringType())                    .add("lpep_pickup_datetime", t.StringType()) \
    .add("lpep_dropoff_datetime", t.StringType())       .add("store_and_fwd_flag", t.StringType()) \
    .add("ratecodeid", t.StringType())                  .add("pickup_longitude", t.StringType()) \
    .add("pickup_latitude", t.StringType())             .add("dropoff_longitude", t.StringType()) \
    .add("dropoff_latitude", t.StringType())            .add("passenger_count", t.StringType()) \
    .add("trip_distance", t.StringType())               .add("fare_amount", t.StringType()) \
    .add("extra", t.StringType())                       .add("mta_tax", t.StringType()) \
    .add("tip_amount", t.StringType())                  .add("tolls_amount", t.StringType()) \
    .add("improvement_surcharge", t.StringType())       .add("total_amount", t.StringType())\
    .add("payment_type", t.StringType())                .add("trip_type", t.StringType())

#==============================================================================================
#==========================change json to dataframe with schema==============================#
taxiTripsDF = socketDF.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")

#====# 1: Remove spaces from column names====================================================#
taxiTripsDF = taxiTripsDF \
    .withColumnRenamed("vendorid", "vendorid")                          .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")     .withColumnRenamed("passenger_count", "passenger_count") \
    .withColumnRenamed("trip_distance", "trip_distance")                .withColumnRenamed("ratecodeid", "ratecodeid") \
    .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")      .withColumnRenamed("payment_type", "PaymentType")


#============================================================================================#
#==== 3: Add date columns from timestamp=====================================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF.withColumn('TripStartDT', taxiTripsDF['pickup_datetime'].cast('date'))
taxiTripsDF = taxiTripsDF.withColumn('TripEndDT', taxiTripsDF['dropoff_datetime'].cast('date'))

#============================================================================================#
#==== 4: Add/convert/casting Additional columns types=========================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF\
    .withColumn('trip_distance', taxiTripsDF['trip_distance'].cast('double'))\
    .withColumn('pickup_longitude', taxiTripsDF['pickup_longitude'].cast('double')) \
    .withColumn('pickup_latitude', taxiTripsDF['pickup_latitude'].cast('double')) \
    .withColumn('dropoff_longitude', taxiTripsDF['dropoff_longitude'].cast('double')) \
    .withColumn('dropoff_latitude', taxiTripsDF['dropoff_latitude'].cast('double'))

taxiTripsDF = taxiTripsDF\
    .withColumn("hourd", hour(taxiTripsDF["pickup_datetime"])) \
    .withColumn("minuted", minute(taxiTripsDF["pickup_datetime"])) \
    .withColumn("secondd", second(taxiTripsDF["pickup_datetime"]))\
    .withColumn("dayofweek", dayofweek(taxiTripsDF["TripStartDT"])) \
    .withColumn("week_day_full", date_format(col("TripStartDT"), "EEEE"))

## CREATE A CLEANED DATA-FRAME BY DROPPING SOME UN-NECESSARY COLUMNS & FILTERING FOR UNDESIRED VALUES OR OUTLIERS
taxiTripsDF = taxiTripsDF\
    .drop('store_and_fwd_flag','fare_amount','extra','tolls_amount','mta_tax','improvement_surcharge','trip_type','ratecodeid','pickup_datetime','dropoff_datetime','PaymentType','TripStartDT','TripEndDT')\
    .filter("passenger_count > 0 and passenger_count < 8  AND tip_amount >= 0 AND tip_amount < 30 AND fare_amount >= 1 AND fare_amount < 150 AND trip_distance > 0 AND trip_distance < 100 ")


# taxiTripsDF.printSchema()
# taxiTripsDF.show(8,False)


taxiTripsDF.printSchema()



# hdfs_query = taxiTripsDF\
#     .writeStream\
#     .format("parquet")\
#     .partitionBy("vendorid")\
#     .option("path",'s3a://nyc/data/')\
#     .option("checkpointLocation", 's3a://nyc/checkpoints/taxi/log')\
#     .outputMode("append")\
#     .start()

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/postgres"  
postgres_properties = {
    "user": "postgres",    
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Define the function to handle each micro-batch of the streaming data
def write_to_postgres(batch_df, batch_id):
    # Write each batch of data to PostgreSQL
    batch_df.write \
        .jdbc(
            url=postgres_url,
            table="taxi_trips",  # Target table name
            mode="append",       # Use 'append' to add data without overwriting
            properties=postgres_properties
        )

# Stream the DataFrame and write to PostgreSQL using the foreachBatch function
streaming_query = taxiTripsDF.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Wait for the termination of the streaming query
streaming_query.awaitTermination()